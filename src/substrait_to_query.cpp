/*
 * substrait_to_query.cpp
 *
 * Converts a serialized Substrait Plan into a PostgreSQL Query* tree.
 * Supported: [SortRel?] > [ProjectRel?] > [AggregateRel?] > [ProjectRel?] > [FilterRel?] > ReadRel|CrossRel|SetRel
 * CrossRel trees (nested cross products) produce multi-table FROM clauses.
 * ReadRel.filter and ReadRel.projection are also handled.
 *
 * Tested against PostgreSQL 18 + protobuf 3.x.
 *
 * INCLUDE ORDER IS CRITICAL — protobuf headers before PG headers.
 * PG's c.h defines ERROR=22 etc. which corrupt protobuf's logging tokens.
 */

/* --- Step 1: C++ standard library (no conflicts) --- */
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

/* --- Step 2: Protobuf / Substrait headers (must be before postgres.h) --- */
#include "substrait/plan.pb.h"
#include "substrait/algebra.pb.h"

/*
 * Step 3: Undefine macros that PostgreSQL defines and that clash with
 * protobuf or standard C++ identifiers.
 *
 * ERROR       — PG defines this as 22 (elevel); protobuf uses LOGLEVEL_ERROR
 * WARNING     — similarly defined by PG
 * INFO/NOTICE — same family
 * vsnprintf   — PG sometimes redefines this on some platforms
 */
#ifdef ERROR
#undef ERROR
#endif
#ifdef WARNING
#undef WARNING
#endif
#ifdef INFO
#undef INFO
#endif
#ifdef NOTICE
#undef NOTICE
#endif
#ifdef DEBUG1
#undef DEBUG1
#endif
#ifdef strtou64
#undef strtou64
#endif

/* --- Step 4: PostgreSQL headers --- */
extern "C"
{
#include "postgres.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_collation_d.h" /* DEFAULT_COLLATION_OID */
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h" /* addRTEPermissionInfo */
#include "parser/parse_oper.h"
#include "catalog/pg_cast.h"
#include "datatype/timestamp.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/catcache.h" /* catclist full definition */
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
}

/* ============================================================
 * SECTION 1 - Schema helpers
 * ============================================================ */

static bool
contains_sublink(Node *node)
{
    if (node == NULL)
        return false;
    if (IsA(node, SubLink))
        return true;
    return expression_tree_walker(node,
        (bool (*)()) contains_sublink, NULL);
}


/* (rtindex, attnum, typeoid) per field.
 * rtindex is 0 as placeholder in read_rel_to_rte; caller fixes up. */
typedef std::vector<std::tuple<int, AttrNumber, Oid>> SubstraitSchema;

/* String type helpers — shared by aggregate, like, and operator coercion. */
static bool
is_string_type(Oid t)
{
    return t == TEXTOID || t == VARCHAROID || t == BPCHAROID;
}

static Expr *
coerce_to_text(Expr *e)
{
    Oid t = exprType((Node *)e);
    if (t == TEXTOID)
        return e;
    RelabelType *r = makeNode(RelabelType);
    r->arg = e;
    r->resulttype = TEXTOID;
    r->resulttypmod = -1;
    r->resultcollid = DEFAULT_COLLATION_OID;
    r->relabelformat = COERCE_IMPLICIT_CAST;
    r->location = -1;
    return (Expr *)r;
}

static List *
build_star_tlist(RangeTblEntry *rte, int rtindex)
{
    List *tlist = NIL;
    AttrNumber resno = 1;

    Relation rel = RelationIdGetRelation(rte->relid);
    TupleDesc tupdesc = RelationGetDescr(rel);

    for (int j = 0; j < tupdesc->natts; j++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, j);
        if (attr->attisdropped)
            continue;

        Var *var = makeVar(rtindex, attr->attnum, attr->atttypid,
                           attr->atttypmod, attr->attcollation, 0);

        TargetEntry *tle = makeTargetEntry((Expr *)var, resno++,
                                           pstrdup(NameStr(attr->attname)),
                                           false);
        tlist = lappend(tlist, tle);
    }

    RelationClose(rel);
    return tlist;
}

/* ============================================================
 * SECTION 2 - Extension / function anchor map
 * ============================================================ */

typedef std::unordered_map<uint32_t, std::string> FuncMap;

static FuncMap
build_func_map(const substrait::Plan &plan)
{
    FuncMap m;
    for (const auto &ext : plan.extensions())
    {
        if (!ext.has_extension_function())
            continue;
        const auto &f = ext.extension_function();
        std::string name = f.name();
        auto colon = name.find(':');
        if (colon != std::string::npos)
            name = name.substr(0, colon);
        m[f.function_anchor()] = name;
    }
    return m;
}

/* ============================================================
 * SECTION 3 - ReadRel to RangeTblEntry
 *
 * PG 16+ moved per-RTE permissions into RTEPermissionInfo.
 * We pass Query* so we can call addRTEPermissionInfo().
 * ============================================================ */

static RangeTblEntry *
read_rel_to_rte(const substrait::ReadRel &read,
                SubstraitSchema &schema_out,
                Query *query)
{
    if (!read.has_named_table())
        ereport(ERROR, (errmsg("substrait: only named-table reads are supported")));

    const auto &names = read.named_table().names();
    if (names.empty())
        ereport(ERROR, (errmsg("substrait: named table has no names")));

    List *relname = NIL;
    for (const auto &n : names)
        relname = lappend(relname, makeString(pstrdup(n.c_str())));

    RangeVar *rv = makeRangeVarFromNameList(relname);
    Oid reloid = RangeVarGetRelid(rv, AccessShareLock, false);

    Relation rel = RelationIdGetRelation(reloid);
    if (!RelationIsValid(rel))
        ereport(ERROR, (errmsg("substrait: could not open relation %u", reloid)));

    TupleDesc tupdesc = RelationGetDescr(rel);
    List *colnames = NIL;

    if (read.has_base_schema())
    {
        const auto &bs = read.base_schema();
        for (int i = 0; i < bs.names_size(); i++)
        {
            const std::string &col = bs.names(i);
            bool found = false;
            for (int j = 0; j < tupdesc->natts; j++)
            {
                Form_pg_attribute attr = TupleDescAttr(tupdesc, j);
                if (attr->attisdropped)
                    continue;
                if (NameStr(attr->attname) == col)
                {
                    schema_out.push_back({0, attr->attnum, attr->atttypid});
                    colnames = lappend(colnames,
                                       makeString(pstrdup(col.c_str())));
                    found = true;
                    break;
                }
            }
            if (!found)
                ereport(ERROR,
                        (errmsg("substrait: column \"%s\" not found in \"%s\"",
                                col.c_str(), names[0].c_str())));
        }
    }
    else
    {
        for (int j = 0; j < tupdesc->natts; j++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, j);
            if (attr->attisdropped)
                continue;
            schema_out.push_back({0, attr->attnum, attr->atttypid});
            colnames = lappend(colnames,
                               makeString(pstrdup(NameStr(attr->attname))));
        }
    }

    RelationClose(rel);

    RangeTblEntry *rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RELATION;
    rte->relid = reloid;
    rte->relkind = RELKIND_RELATION;
    rte->rellockmode = AccessShareLock;
    rte->lateral = false;
    rte->inh = true;
    rte->inFromCl = true;
    rte->alias = NULL;
    rte->eref = makeAlias(pstrdup(names[0].c_str()), colnames);

    /* PG 16+: permissions moved to RTEPermissionInfo */
    RTEPermissionInfo *perminfo = addRTEPermissionInfo(&query->rteperminfos, rte);
    perminfo->requiredPerms = ACL_SELECT;

    return rte;
}

/* Virtual expression map: field_index → precomputed PG Expr.
 * Used for inner ProjectRel computed columns. */
typedef std::unordered_map<int, Expr *> VirtualMap;

/* Forward declarations — defined in Sections 5 and 6. */
static Expr *
convert_expression(const substrait::Expression &expr,
                   const SubstraitSchema &schema,
                   const FuncMap &func_map,
                   const VirtualMap &virtuals = {},
                   const SubstraitSchema *outer_schema = nullptr,
                   Query *window_query = nullptr);

static Query *
build_query_for_rel(const substrait::Rel *cur, const FuncMap &func_map,
                    const substrait::RelRoot *root,
                    const SubstraitSchema *outer_schema = nullptr);

/* Build a from-list Node for a base relation tree.
 * Adds RTEs to rtable. Returns a single Node* for Read/Join;
 * for Cross (and flattened INNER Join), adds children to fromlist
 * directly and returns NULL.
 * flatten_inner: when true, INNER JoinRel is flattened like CrossRel
 * (conditions moved to WHERE). Set false when parent needs a non-NULL
 * node (e.g. larg/rarg of a non-INNER JoinExpr).
 * virtuals_out: if non-null, receives computed column expressions from
 * inlined ProjectRel (key = field index in schema_out). */
static Node *
build_from_node(const substrait::Rel &rel, Query *query,
                SubstraitSchema &schema_out, const FuncMap &func_map,
                bool flatten_inner = true,
                VirtualMap *virtuals_out = nullptr)
{
    if (rel.has_read())
    {
        SubstraitSchema local;
        RangeTblEntry *rte = read_rel_to_rte(rel.read(), local, query);
        query->rtable = lappend(query->rtable, rte);
        int rtindex = list_length(query->rtable);

        for (auto &[rt, attnum, typeoid] : local)
            rt = rtindex;
        schema_out.insert(schema_out.end(), local.begin(), local.end());

        RangeTblRef *rtr = makeNode(RangeTblRef);
        rtr->rtindex = rtindex;
        return (Node *)rtr;
    }
    else if (rel.has_cross())
    {
        const auto &cross = rel.cross();
        VirtualMap left_virt, right_virt;
        int left_base = (int)schema_out.size();
        Node *l = build_from_node(cross.left(), query, schema_out, func_map,
                                  true, &left_virt);
        if (l)
            query->jointree->fromlist = lappend(query->jointree->fromlist, l);
        int left_count = (int)schema_out.size() - left_base;
        Node *r = build_from_node(cross.right(), query, schema_out, func_map,
                                  true, &right_virt);
        if (r)
            query->jointree->fromlist = lappend(query->jointree->fromlist, r);

        /* Merge virtual maps: right side offset by left column count. */
        if (virtuals_out)
        {
            *virtuals_out = left_virt;
            for (auto &[idx, expr] : right_virt)
                (*virtuals_out)[idx + left_count] = expr;
        }
        return NULL;
    }
    else if (rel.has_join())
    {
        const auto &jn = rel.join();

        /* Flatten INNER JoinRel like CrossRel: add children to fromlist,
         * move join condition to WHERE clause. Only when caller can handle
         * NULL return (top-level, CrossRel parent, or another flattened
         * INNER parent). */
        if (flatten_inner &&
            jn.type() == substrait::JoinRel::JOIN_TYPE_INNER)
        {
            SubstraitSchema left_schema, right_schema;
            VirtualMap left_virt, right_virt;
            Node *l = build_from_node(jn.left(), query, left_schema, func_map,
                                      true, &left_virt);
            if (l)
                query->jointree->fromlist =
                    lappend(query->jointree->fromlist, l);
            Node *r = build_from_node(jn.right(), query, right_schema,
                                      func_map, true, &right_virt);
            if (r)
                query->jointree->fromlist =
                    lappend(query->jointree->fromlist, r);

            schema_out = left_schema;
            schema_out.insert(schema_out.end(), right_schema.begin(),
                              right_schema.end());

            /* Merge virtual maps: right side offset by left column count. */
            VirtualMap combined_virt = left_virt;
            int left_count = (int)left_schema.size();
            for (auto &[idx, expr] : right_virt)
                combined_virt[idx + left_count] = expr;

            /* Move join condition to WHERE clause. */
            if (jn.has_expression())
            {
                Node *cond = (Node *)convert_expression(
                    jn.expression(), schema_out, func_map, combined_virt);
                if (query->jointree->quals == NULL)
                    query->jointree->quals = cond;
                else
                    query->jointree->quals = (Node *)makeBoolExpr(
                        AND_EXPR,
                        list_make2(query->jointree->quals, cond), -1);
            }

            if (virtuals_out)
                *virtuals_out = combined_virt;

            return NULL;
        }

        /* Non-INNER joins (LEFT/RIGHT/FULL) or non-flattenable INNER:
         * build explicit JoinExpr. Children called with flatten_inner=false
         * since JoinExpr requires non-NULL larg/rarg. */
        SubstraitSchema left_schema, right_schema;
        VirtualMap left_virt, right_virt;

        Node *larg = build_from_node(jn.left(), query, left_schema, func_map,
                                     false, &left_virt);
        Node *rarg = build_from_node(jn.right(), query, right_schema, func_map,
                                     false, &right_virt);

        /* Combined schema for join condition resolution. */
        SubstraitSchema combined = left_schema;
        combined.insert(combined.end(), right_schema.begin(),
                        right_schema.end());

        JoinType pg_jtype;
        switch (jn.type())
        {
            case substrait::JoinRel::JOIN_TYPE_INNER:
                pg_jtype = JOIN_INNER; break;
            case substrait::JoinRel::JOIN_TYPE_LEFT:
                pg_jtype = JOIN_LEFT; break;
            case substrait::JoinRel::JOIN_TYPE_RIGHT:
                pg_jtype = JOIN_RIGHT; break;
            case substrait::JoinRel::JOIN_TYPE_OUTER:
                pg_jtype = JOIN_FULL; break;
            default:
                ereport(ERROR,
                        (errmsg("substrait: unsupported join type %d",
                                jn.type())));
        }

        /* Merge virtual maps for join condition resolution. */
        VirtualMap combined_virt = left_virt;
        int left_count = (int)left_schema.size();
        for (auto &[idx, expr] : right_virt)
            combined_virt[idx + left_count] = expr;

        Node *quals = NULL;
        if (jn.has_expression())
            quals = (Node *)convert_expression(jn.expression(),
                                               combined, func_map,
                                               combined_virt);

        /* RTE_JOIN entry for the planner. */
        List *alias_vars = NIL;
        List *col_names = NIL;
        for (int ci = 0; ci < (int)combined.size(); ci++)
        {
            auto &[rt, attnum, typeoid] = combined[ci];
            Oid collation = is_string_type(typeoid)
                ? DEFAULT_COLLATION_OID : InvalidOid;
            auto vit = combined_virt.find(ci);
            if (vit != combined_virt.end())
                alias_vars = lappend(alias_vars,
                    copyObjectImpl(vit->second));
            else
                alias_vars = lappend(alias_vars,
                    makeVar(rt, attnum, typeoid, -1, collation, 0));
            col_names = lappend(col_names,
                makeString(pstrdup("?column?")));
        }

        RangeTblEntry *join_rte = makeNode(RangeTblEntry);
        join_rte->rtekind = RTE_JOIN;
        join_rte->jointype = pg_jtype;
        join_rte->joinmergedcols = 0;
        join_rte->joinaliasvars = alias_vars;
        join_rte->joinleftcols = NIL;
        join_rte->joinrightcols = NIL;
        join_rte->join_using_alias = NULL;
        join_rte->eref = makeAlias(pstrdup("unnamed_join"), col_names);
        join_rte->lateral = false;
        join_rte->inh = false;
        join_rte->inFromCl = true;

        query->rtable = lappend(query->rtable, join_rte);
        int join_rtindex = list_length(query->rtable);

        JoinExpr *jexpr = makeNode(JoinExpr);
        jexpr->jointype = pg_jtype;
        jexpr->isNatural = false;
        jexpr->larg = larg;
        jexpr->rarg = rarg;
        jexpr->usingClause = NIL;
        jexpr->join_using_alias = NULL;
        jexpr->quals = quals;
        jexpr->alias = NULL;
        jexpr->rtindex = join_rtindex;

        schema_out = combined;
        return (Node *)jexpr;
    }

    /* --- FilterRel: peel filter, recurse on input, add condition to WHERE.
     * Avoids subquery barrier for filtered base tables inside joins. --- */
    if (rel.has_filter())
    {
        const auto &filt = rel.filter();
        Node *node = build_from_node(filt.input(), query, schema_out,
                                     func_map, flatten_inner, virtuals_out);
        if (filt.has_condition())
        {
            VirtualMap vmap;
            if (virtuals_out)
                vmap = *virtuals_out;
            Node *cond = (Node *)convert_expression(
                filt.condition(), schema_out, func_map, vmap);
            if (query->jointree->quals == NULL)
                query->jointree->quals = cond;
            else
                query->jointree->quals = (Node *)makeBoolExpr(
                    AND_EXPR,
                    list_make2(query->jointree->quals, cond), -1);
        }
        return node;
    }

    /* --- ProjectRel: peel projection, recurse on input, apply emit mapping.
     * Inlines column selection/reordering and computed columns.
     * Computed columns stored in virtuals_out for join condition resolution. --- */
    if (rel.has_project())
    {
        const auto &proj = rel.project();
        SubstraitSchema input_schema;
        VirtualMap input_virtuals;
        Node *node = build_from_node(proj.input(), query, input_schema,
                                     func_map, flatten_inner, &input_virtuals);

        /* Convert expressions (computed columns). */
        std::vector<Expr *> raw_virtuals;
        for (int i = 0; i < proj.expressions_size(); i++)
        {
            raw_virtuals.push_back(convert_expression(
                proj.expressions(i), input_schema, func_map, input_virtuals));
        }

        int n_input = (int)input_schema.size();

        if (proj.has_common() && proj.common().has_emit())
        {
            const auto &mapping = proj.common().emit().output_mapping();
            schema_out.clear();
            VirtualMap new_virtuals;
            for (int i = 0; i < mapping.size(); i++)
            {
                int src = mapping.Get(i);
                if (src < n_input)
                {
                    schema_out.push_back(input_schema[src]);
                    auto vit = input_virtuals.find(src);
                    if (vit != input_virtuals.end())
                        new_virtuals[i] = vit->second;
                }
                else
                {
                    Expr *ve = raw_virtuals[src - n_input];
                    schema_out.push_back({0, 0, exprType((Node *)ve)});
                    new_virtuals[i] = ve;
                }
            }
            if (virtuals_out)
                *virtuals_out = new_virtuals;
        }
        else
        {
            schema_out = input_schema;
            VirtualMap new_virtuals = input_virtuals;
            for (size_t i = 0; i < raw_virtuals.size(); i++)
            {
                Expr *ve = raw_virtuals[i];
                schema_out.push_back({0, 0, exprType((Node *)ve)});
                new_virtuals[n_input + (int)i] = ve;
            }
            if (virtuals_out)
                *virtuals_out = new_virtuals;
        }

        return node;
    }

    /* Non-base rel (e.g. Aggregate inside a Cross) — wrap as subquery. */
    Query *subq = build_query_for_rel(&rel, func_map, nullptr, nullptr);

    RangeTblEntry *sub_rte = makeNode(RangeTblEntry);
    sub_rte->rtekind = RTE_SUBQUERY;
    sub_rte->subquery = subq;
    sub_rte->lateral = false;
    sub_rte->inh = false;
    sub_rte->inFromCl = true;

    List *colnames = NIL;
    int attno = 1;
    ListCell *lc;
    foreach(lc, subq->targetList)
    {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        colnames = lappend(colnames,
            makeString(pstrdup(tle->resname ? tle->resname : "?column?")));
        Oid t = exprType((Node *)tle->expr);
        schema_out.push_back({0, (AttrNumber)attno, t});
        attno++;
    }
    sub_rte->eref = makeAlias(pstrdup("subq"), colnames);

    query->rtable = lappend(query->rtable, sub_rte);
    int sub_rtindex = list_length(query->rtable);

    for (auto &[rt, attnum, typeoid] : schema_out)
        if (rt == 0) rt = sub_rtindex;

    RangeTblRef *rtr = makeNode(RangeTblRef);
    rtr->rtindex = sub_rtindex;
    return (Node *)rtr;
}

/* Top-level: process base relation tree into query->jointree->fromlist. */
static void
process_base_rel(const substrait::Rel &rel, Query *query,
                 SubstraitSchema &schema_out, const FuncMap &func_map,
                 VirtualMap *virtuals_out = nullptr)
{
    Node *node = build_from_node(rel, query, schema_out, func_map,
                                 true, virtuals_out);
    if (node)
        query->jointree->fromlist = lappend(query->jointree->fromlist, node);
}

/* ============================================================
 * SECTION 4 - AggregateRel to targetList
 * ============================================================ */

static const std::unordered_map<std::string, std::string> kAggNameMap = {
    {"count", "count"},
    {"count_star", "count"},
    {"sum", "sum"},
    {"avg", "avg"},
    {"min", "min"},
    {"max", "max"},
    {"stddev", "stddev"},
    {"variance", "var_samp"},
    {"bool_and", "bool_and"},
    {"bool_or", "bool_or"},
};

/*
 * Find count(*) in pg_proc.
 *
 * pg_aggregate no longer has aggstar in PG 18.
 * We identify count(*) as the "count" function in pg_catalog whose
 * single argument type is ANYOID — that is the standard sentinel
 * for star/variadic aggregates.
 */
static Oid
lookup_count_star(void)
{
    Oid nspoid = LookupExplicitNamespace("pg_catalog", false);
    Oid result = InvalidOid;

    /* catclist is the PG 17+ lowercase spelling of CatCList */
    catclist *cl = SearchSysCacheList1(PROCNAMEARGSNSP,
                                       CStringGetDatum("count"));

    for (int i = 0; i < cl->n_members; i++)
    {
        HeapTuple tup = &cl->members[i]->tuple;
        Form_pg_proc proc = (Form_pg_proc)GETSTRUCT(tup);

        if (proc->pronamespace != nspoid)
            continue;
        if (proc->pronargs != 1)
            continue;

        /* count(*) has argument type "any" (ANYOID) */
        if (proc->proargtypes.values[0] == ANYOID)
        {
            result = proc->oid;
            break;
        }
    }

    ReleaseSysCacheList(cl);

    if (!OidIsValid(result))
        ereport(ERROR,
                (errmsg("substrait: could not find count(*) in pg_proc")));

    return result;
}

static List *
aggregate_rel_to_targetlist(const substrait::AggregateRel &agg,
                            Query *query,
                            const SubstraitSchema &schema,
                            const FuncMap &func_map,
                            const VirtualMap &virtuals = {})
{
    query->hasAggs = true;

    List *tlist = NIL;
    AttrNumber resno = 1;

    /* --- Grouping keys --- */
    for (const auto &grouping : agg.groupings())
    {
        /* Suppress deprecation warning for grouping_expressions() */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
        for (const auto &gexpr : grouping.grouping_expressions())
        {
#pragma GCC diagnostic pop
            if (!gexpr.has_selection() ||
                !gexpr.selection().has_direct_reference() ||
                !gexpr.selection().direct_reference().has_struct_field())
                ereport(ERROR,
                        (errmsg("substrait: non-field grouping key not supported")));

            int field_idx =
                gexpr.selection().direct_reference().struct_field().field();

            if (field_idx < 0 || field_idx >= (int)schema.size())
                ereport(ERROR,
                        (errmsg("substrait: grouping field index %d out of range",
                                field_idx)));

            auto [rt, attnum, typeoid] = schema[field_idx];

            Expr *group_expr;
            char *attname;
            auto vit = virtuals.find(field_idx);
            if (vit != virtuals.end())
            {
                group_expr = (Expr *)copyObjectImpl(vit->second);
                attname = pstrdup("expr");
            }
            else
            {
                Oid collation = is_string_type(typeoid)
                    ? DEFAULT_COLLATION_OID : InvalidOid;
                group_expr = (Expr *)makeVar(rt, attnum, typeoid,
                                             -1, collation, 0);
                RangeTblEntry *field_rte =
                    (RangeTblEntry *)list_nth(query->rtable, rt - 1);
                attname = get_attname(field_rte->relid, attnum, false);
            }

            Index sortgroupref = (Index)resno;

            TargetEntry *tle = makeTargetEntry(group_expr, resno++,
                                               attname, false);
            tle->ressortgroupref = sortgroupref;
            tlist = lappend(tlist, tle);

            Oid eqop, sortop;
            bool hashable;
            get_sort_group_operators(typeoid, false, true, false,
                                     &sortop, &eqop, NULL, &hashable);

            SortGroupClause *sgc = makeNode(SortGroupClause);
            sgc->tleSortGroupRef = sortgroupref;
            sgc->eqop = eqop;
            sgc->sortop = sortop;
            sgc->nulls_first = false;
            sgc->hashable = hashable;
            query->groupClause = lappend(query->groupClause, sgc);
        }
    }

    /* --- Aggregate measures --- */
    for (const auto &measure : agg.measures())
    {
        const auto &amf = measure.measure();

        auto fit = func_map.find(amf.function_reference());
        if (fit == func_map.end())
            ereport(ERROR,
                    (errmsg("substrait: unknown aggregate function anchor %u",
                            amf.function_reference())));

        const std::string &substrait_name = fit->second;
        auto nit = kAggNameMap.find(substrait_name);
        const std::string pg_agg =
            (nit != kAggNameMap.end()) ? nit->second : substrait_name;

        bool is_star = (substrait_name == "count_star" ||
                        (pg_agg == "count" && amf.arguments_size() == 0));

        /* Convert arguments once, derive types from converted exprs. */
        std::vector<Expr *> converted_args;
        std::vector<Oid> argtypes;
        Oid aggfnoid = InvalidOid;
        if (is_star)
        {
            aggfnoid = lookup_count_star();
        }
        else
        {
            for (const auto &arg : amf.arguments())
            {
                if (!arg.has_value())
                    ereport(ERROR,
                            (errmsg("substrait: enum/type agg args unsupported")));
                Expr *e = convert_expression(
                    arg.value(), schema, func_map, virtuals);
                converted_args.push_back(e);
                argtypes.push_back(exprType((Node *)e));
            }
            /* count("any") — polymorphic, same pg_proc entry as count(*). */
            if (pg_agg == "count")
                aggfnoid = lookup_count_star();
            else
            {
                List *funcname = list_make1(makeString(pstrdup(pg_agg.c_str())));
                aggfnoid = LookupFuncName(funcname, (int)argtypes.size(),
                                          argtypes.data(), false);
            }
        }

        HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(aggfnoid));
        if (!HeapTupleIsValid(proctup))
            ereport(ERROR,
                    (errmsg("substrait: pg_proc entry missing for OID %u",
                            aggfnoid)));
        Oid rettype = ((Form_pg_proc)GETSTRUCT(proctup))->prorettype;
        ReleaseSysCache(proctup);

        bool is_distinct =
            (!is_star &&
             amf.invocation() ==
             substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT);

        List *agg_args = NIL;
        List *aggargtypes = NIL;
        List *distinctList = NIL;
        if (!is_star)
        {
            for (size_t i = 0; i < converted_args.size(); i++)
            {
                TargetEntry *arg_tle =
                    makeTargetEntry(converted_args[i],
                                    (AttrNumber)(i + 1),
                                    NULL, false);
                if (is_distinct)
                    arg_tle->ressortgroupref = (Index)(i + 1);
                agg_args = lappend(agg_args, arg_tle);
            }
            for (Oid oid : argtypes)
                aggargtypes = lappend_oid(aggargtypes, oid);

            if (is_distinct)
            {
                for (size_t i = 0; i < argtypes.size(); i++)
                {
                    Oid eqop, sortop;
                    bool hashable;
                    get_sort_group_operators(argtypes[i], false, true, false,
                                             &sortop, &eqop, NULL, &hashable);
                    SortGroupClause *sgc = makeNode(SortGroupClause);
                    sgc->tleSortGroupRef = (Index)(i + 1);
                    sgc->eqop = eqop;
                    sgc->sortop = sortop;
                    sgc->nulls_first = false;
                    sgc->hashable = hashable;
                    distinctList = lappend(distinctList, sgc);
                }
            }
        }

        Aggref *aggref = makeNode(Aggref);
        aggref->aggfnoid = aggfnoid;
        aggref->aggtype = rettype;
        aggref->aggcollid = InvalidOid;
        aggref->inputcollid = InvalidOid;
        aggref->aggtranstype = InvalidOid;
        aggref->aggargtypes = aggargtypes;
        aggref->aggdirectargs = NIL;
        aggref->args = agg_args;
        aggref->aggorder = NIL;
        aggref->aggdistinct = distinctList;
        aggref->aggfilter = NULL;
        aggref->aggstar = is_star;
        aggref->aggvariadic = false;
        aggref->aggkind = AGGKIND_NORMAL;
        aggref->agglevelsup = 0;
        aggref->aggsplit = AGGSPLIT_SIMPLE;
        aggref->location = -1;

        TargetEntry *tle = makeTargetEntry(
            (Expr *)aggref,
            resno++,
            pstrdup(substrait_name.c_str()),
            false);
        tlist = lappend(tlist, tle);
    }

    return tlist;
}

/* ============================================================
 * SECTION 5 - General expression converter
 *
 * Converts a substrait Expression into a PG Expr* tree.
 * Used by FilterRel, AggregateRel args, and ProjectRel expressions.
 * ============================================================ */

/* Map substrait scalar function names to PG operator symbols. */
static const std::unordered_map<std::string, const char *> kOpNameMap = {
    {"gt", ">"},     {"lt", "<"},     {"gte", ">="},   {"lte", "<="},
    {"equal", "="},  {"not_equal", "<>"},
    {"add", "+"},    {"subtract", "-"},
    {"multiply", "*"}, {"divide", "/"},
};

static Expr *
convert_expression(const substrait::Expression &expr,
                   const SubstraitSchema &schema,
                   const FuncMap &func_map,
                   const VirtualMap &virtuals,
                   const SubstraitSchema *outer_schema,
                   Query *window_query)
{
    /* --- FieldReference → Var (or virtual expression) --- */
    if (expr.has_selection())
    {
        const auto &sel = expr.selection();
        if (!sel.has_direct_reference() ||
            !sel.direct_reference().has_struct_field())
            ereport(ERROR,
                    (errmsg("substrait: unsupported field reference type")));

        int idx = sel.direct_reference().struct_field().field();

        /* Outer reference: column from enclosing query (correlated subquery). */
        if (sel.has_outer_reference())
        {
            if (!outer_schema || idx < 0 || idx >= (int)outer_schema->size())
                ereport(ERROR,
                        (errmsg("substrait: outer reference %d out of range",
                                idx)));
            auto [rt, attnum, typeoid] = (*outer_schema)[idx];
            Oid collation = is_string_type(typeoid)
                ? DEFAULT_COLLATION_OID : InvalidOid;
            Var *v = makeVar(rt, attnum, typeoid, -1, collation, 0);
            v->varlevelsup = sel.outer_reference().steps_out();
            return (Expr *)v;
        }

        if (idx < 0 || idx >= (int)schema.size())
            ereport(ERROR,
                    (errmsg("substrait: field index %d out of range", idx)));

        /* Virtual column from inner ProjectRel — return copy of stored expr. */
        auto vit = virtuals.find(idx);
        if (vit != virtuals.end())
            return (Expr *)copyObjectImpl(vit->second);

        auto [rt, attnum, typeoid] = schema[idx];
        Oid collation = is_string_type(typeoid)
            ? DEFAULT_COLLATION_OID : InvalidOid;

        return (Expr *)makeVar(rt, attnum, typeoid, -1, collation, 0);
    }

    /* --- Subquery → SubLink --- */
    if (expr.has_subquery())
    {
        const auto &sq = expr.subquery();
        if (sq.has_scalar())
        {
            Query *subq = build_query_for_rel(
                &sq.scalar().input(), func_map, nullptr, &schema);

            SubLink *sl = makeNode(SubLink);
            sl->subLinkType = EXPR_SUBLINK;
            sl->subLinkId = 0;
            sl->testexpr = NULL;
            sl->operName = NIL;
            sl->subselect = (Node *)subq;
            sl->location = -1;
            return (Expr *)sl;
        }

        /* EXISTS (subquery) */
        if (sq.has_set_predicate())
        {
            const auto &sp = sq.set_predicate();
            Query *subq = build_query_for_rel(
                &sp.tuples(), func_map, nullptr, &schema);

            SubLink *sl = makeNode(SubLink);
            sl->subLinkType = EXISTS_SUBLINK;
            sl->subLinkId = 0;
            sl->testexpr = NULL;
            sl->operName = NIL;
            sl->subselect = (Node *)subq;
            sl->location = -1;
            return (Expr *)sl;
        }

        /* x IN (subquery) → ANY_SUBLINK */
        if (sq.has_in_predicate())
        {
            const auto &inp = sq.in_predicate();
            Query *subq = build_query_for_rel(
                &inp.haystack(), func_map, nullptr, &schema);

            /* Build testexpr: needle = Param(PARAM_SUBLINK) */
            Expr *needle = convert_expression(
                inp.needles(0), schema, func_map, virtuals, outer_schema);
            Oid needle_type = exprType((Node *)needle);

            TargetEntry *ste = (TargetEntry *)linitial(subq->targetList);
            Oid subq_type = exprType((Node *)ste->expr);

            Param *param = makeNode(Param);
            param->paramkind = PARAM_SUBLINK;
            param->paramid = 1;
            param->paramtype = subq_type;
            param->paramtypmod = -1;
            param->paramcollid = is_string_type(subq_type)
                ? DEFAULT_COLLATION_OID : InvalidOid;
            param->location = -1;

            /* Coerce types if needed for operator lookup. */
            Oid ltype = needle_type, rtype = subq_type;
            if (is_string_type(ltype) && is_string_type(rtype) &&
                ltype != rtype)
            {
                needle = coerce_to_text(needle);
                ltype = TEXTOID;
                param->paramtype = TEXTOID;
                param->paramcollid = DEFAULT_COLLATION_OID;
                rtype = TEXTOID;
            }

            List *opname = list_make1(makeString(pstrdup("=")));
            Oid opoid = LookupOperName(NULL, opname, ltype, rtype,
                                       false, -1);
            HeapTuple optup = SearchSysCache1(OPEROID,
                                              ObjectIdGetDatum(opoid));
            Form_pg_operator opform = (Form_pg_operator)GETSTRUCT(optup);

            OpExpr *op = makeNode(OpExpr);
            op->opno = opoid;
            op->opfuncid = opform->oprcode;
            op->opresulttype = BOOLOID;
            op->opretset = false;
            op->opcollid = InvalidOid;
            op->inputcollid = (is_string_type(ltype) || is_string_type(rtype))
                ? DEFAULT_COLLATION_OID : InvalidOid;
            op->args = list_make2(needle, param);
            op->location = -1;
            ReleaseSysCache(optup);

            SubLink *sl = makeNode(SubLink);
            sl->subLinkType = ANY_SUBLINK;
            sl->subLinkId = 0;
            sl->testexpr = (Node *)op;
            sl->operName = list_make1(makeString(pstrdup("=")));
            sl->subselect = (Node *)subq;
            sl->location = -1;
            return (Expr *)sl;
        }

        ereport(ERROR,
                (errmsg("substrait: unsupported subquery type")));
    }

    /* --- Literal → Const --- */
    if (expr.has_literal())
    {
        const auto &lit = expr.literal();

        /* NULL literal — type info is in lit.null(), but PG just needs
         * a Const with isnull=true.  Use UNKNOWNOID; the planner casts. */
        if (lit.has_null())
            return (Expr *)makeConst(UNKNOWNOID, -1, InvalidOid, -1,
                                     (Datum)0, true, false);

        if (lit.has_boolean())
            return (Expr *)makeConst(BOOLOID, -1, InvalidOid, 1,
                                     BoolGetDatum(lit.boolean()),
                                     false, true);
        if (lit.has_i8())
            return (Expr *)makeConst(INT2OID, -1, InvalidOid, 2,
                                     Int16GetDatum((int16)lit.i8()),
                                     false, true);
        if (lit.has_i16())
            return (Expr *)makeConst(INT2OID, -1, InvalidOid, 2,
                                     Int16GetDatum((int16)lit.i16()),
                                     false, true);
        if (lit.has_i32())
            return (Expr *)makeConst(INT4OID, -1, InvalidOid, 4,
                                     Int32GetDatum(lit.i32()),
                                     false, true);
        if (lit.has_i64())
            return (Expr *)makeConst(INT8OID, -1, InvalidOid, 8,
                                     Int64GetDatum(lit.i64()),
                                     false, true);
        if (lit.has_fp32())
            return (Expr *)makeConst(FLOAT4OID, -1, InvalidOid, 4,
                                     Float4GetDatum(lit.fp32()),
                                     false, true);
        if (lit.has_fp64())
            return (Expr *)makeConst(FLOAT8OID, -1, InvalidOid, 8,
                                     Float8GetDatum(lit.fp64()),
                                     false, false);
        if (lit.has_string())
            return (Expr *)makeConst(
                TEXTOID, -1, DEFAULT_COLLATION_OID, -1,
                PointerGetDatum(cstring_to_text(lit.string().c_str())),
                false, false);
        if (lit.has_var_char())
            return (Expr *)makeConst(
                TEXTOID, -1, DEFAULT_COLLATION_OID, -1,
                PointerGetDatum(cstring_to_text(
                    lit.var_char().value().c_str())),
                false, false);
        if (lit.has_fixed_char())
            return (Expr *)makeConst(
                TEXTOID, -1, DEFAULT_COLLATION_OID, -1,
                PointerGetDatum(cstring_to_text(
                    lit.fixed_char().c_str())),
                false, false);
        if (lit.has_date())
        {
            /* Substrait: days since 1970-01-01.
             * PG DateADT: days since 2000-01-01.
             * POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE = 10957. */
            int32 pg_date = lit.date() - 10957;
            return (Expr *)makeConst(DATEOID, -1, InvalidOid, 4,
                                     Int32GetDatum(pg_date),
                                     false, true);
        }
        if (lit.has_decimal())
        {
            const auto &dec = lit.decimal();
            /* Substrait decimal: value is little-endian 16-byte int,
             * precision and scale in the type. Convert via string. */
            const std::string &raw = dec.value();
            /* Read as little-endian 128-bit signed integer */
            __int128 val = 0;
            for (int i = (int)raw.size() - 1; i >= 0; i--)
                val = (val << 8) | (unsigned char)raw[i];

            bool neg = false;
            unsigned __int128 uval;
            if (val < 0)
            {
                neg = true;
                uval = (unsigned __int128)(-(val + 1)) + 1;
            }
            else
                uval = (unsigned __int128)val;

            /* Convert to decimal string */
            char buf[64];
            char *p = buf + sizeof(buf) - 1;
            *p = '\0';
            int32 scale = dec.scale();
            int digits = 0;
            do
            {
                if (digits == scale && scale > 0)
                    *--p = '.';
                *--p = '0' + (int)(uval % 10);
                uval /= 10;
                digits++;
            } while (uval > 0);
            /* Pad with leading zeros if needed for scale */
            while (digits < scale)
            {
                *--p = '0';
                digits++;
            }
            if (digits == scale && scale > 0)
            {
                *--p = '.';
                *--p = '0';
            }
            if (neg)
                *--p = '-';

            int32 precision = dec.precision();
            int32 typmod = ((precision << 16) | scale) + VARHDRSZ;

            Datum numval = DirectFunctionCall3(
                numeric_in,
                CStringGetDatum(p),
                ObjectIdGetDatum(InvalidOid),
                Int32GetDatum(typmod));

            return (Expr *)makeConst(NUMERICOID, typmod, InvalidOid, -1,
                                     numval, false, false);
        }

        if (lit.has_interval_year_to_month())
        {
            const auto &iym = lit.interval_year_to_month();
            Interval *iv = (Interval *)palloc(sizeof(Interval));
            iv->time = 0;
            iv->day = 0;
            iv->month = iym.years() * 12 + iym.months();
            return (Expr *)makeConst(INTERVALOID, -1, InvalidOid,
                                     sizeof(Interval),
                                     PointerGetDatum(iv),
                                     false, false);
        }
        if (lit.has_interval_day_to_second())
        {
            const auto &ids = lit.interval_day_to_second();
            Interval *iv = (Interval *)palloc(sizeof(Interval));
            iv->time = (int64)ids.seconds() * 1000000;
            iv->day = ids.days();
            iv->month = 0;
            return (Expr *)makeConst(INTERVALOID, -1, InvalidOid,
                                     sizeof(Interval),
                                     PointerGetDatum(iv),
                                     false, false);
        }

        ereport(ERROR,
                (errmsg("substrait: unsupported literal type")));
    }

    /* --- IfThen → CaseExpr --- */
    if (expr.has_if_then())
    {
        const auto &ift = expr.if_then();
        CaseExpr *c = makeNode(CaseExpr);
        c->casetype = InvalidOid; /* set below */
        c->casecollid = InvalidOid;
        c->arg = NULL; /* simple CASE (no test expr) */
        c->args = NIL;
        c->defresult = NULL;
        c->location = -1;

        for (int i = 0; i < ift.ifs_size(); i++)
        {
            const auto &clause = ift.ifs(i);
            CaseWhen *w = makeNode(CaseWhen);
            w->expr = convert_expression(clause.if_(), schema,
                                         func_map, virtuals, outer_schema,
                                         window_query);
            w->result = convert_expression(clause.then(), schema,
                                           func_map, virtuals, outer_schema,
                                           window_query);
            w->location = -1;
            c->args = lappend(c->args, w);
        }

        if (ift.has_else_())
            c->defresult = convert_expression(ift.else_(), schema,
                                              func_map, virtuals, outer_schema,
                                              window_query);

        /* Result type from first THEN branch. */
        Oid rtype = exprType((Node *)((CaseWhen *)linitial(c->args))->result);
        c->casetype = rtype;
        if (is_string_type(rtype))
            c->casecollid = DEFAULT_COLLATION_OID;

        return (Expr *)c;
    }

    /* --- Cast → convert input and apply PG coercion --- */
    if (expr.has_cast())
    {
        const auto &cast = expr.cast();
        if (!cast.has_input())
            ereport(ERROR, (errmsg("substrait: cast without input")));

        Expr *input = convert_expression(cast.input(), schema,
                                         func_map, virtuals, outer_schema,
                                         window_query);
        Oid src_type = exprType((Node *)input);

        /* Map substrait target type to PG OID. */
        Oid dst_type = InvalidOid;
        int32 dst_typmod = -1;
        if (cast.has_type())
        {
            const auto &t = cast.type();
            if (t.has_bool_())
                dst_type = BOOLOID;
            else if (t.has_i16())
                dst_type = INT2OID;
            else if (t.has_i32())
                dst_type = INT4OID;
            else if (t.has_i64())
                dst_type = INT8OID;
            else if (t.has_fp32())
                dst_type = FLOAT4OID;
            else if (t.has_fp64())
                dst_type = FLOAT8OID;
            else if (t.has_string())
                dst_type = TEXTOID;
            else if (t.has_date())
                dst_type = DATEOID;
            else if (t.has_decimal())
            {
                dst_type = NUMERICOID;
                int32 precision = t.decimal().precision();
                int32 scale = t.decimal().scale();
                dst_typmod = ((precision << 16) | scale) + VARHDRSZ;
            }
            else if (t.has_varchar())
                dst_type = VARCHAROID;
            else if (t.has_fixed_char())
                dst_type = BPCHAROID;
        }

        if (!OidIsValid(dst_type))
            ereport(ERROR,
                    (errmsg("substrait: unsupported cast target type")));

        /* No-op if types already match. */
        if (src_type == dst_type && dst_typmod == -1)
            return input;

        /* Build a CoerceViaIO or RelabelType depending on cast path. */
        CoercionPathType pathtype;
        Oid funcid;
        pathtype = find_coercion_pathway(dst_type, src_type,
                                         COERCION_EXPLICIT, &funcid);

        if (pathtype == COERCION_PATH_FUNC && OidIsValid(funcid))
        {
            FuncExpr *fe = makeNode(FuncExpr);
            fe->funcid = funcid;
            fe->funcresulttype = dst_type;
            fe->funcretset = false;
            fe->funcvariadic = false;
            fe->funcformat = COERCE_EXPLICIT_CAST;
            fe->funccollid = InvalidOid;
            fe->inputcollid = InvalidOid;
            fe->args = list_make1(input);
            fe->location = -1;
            Expr *result = (Expr *)fe;

            /* Apply typmod coercion if needed (e.g. NUMERIC(15,2)). */
            if (dst_typmod != -1)
            {
                Oid typmod_func = InvalidOid;
                /* For numeric, typmod is enforced at storage;
                 * PG's coercion framework typically handles this. */
                HeapTuple tp = SearchSysCache2(CASTSOURCETARGET,
                                               ObjectIdGetDatum(dst_type),
                                               ObjectIdGetDatum(dst_type));
                if (HeapTupleIsValid(tp))
                {
                    typmod_func = ((Form_pg_cast)GETSTRUCT(tp))->castfunc;
                    ReleaseSysCache(tp);
                }
                (void)typmod_func; /* typmod applied at execution */
            }
            return result;
        }
        else if (pathtype == COERCION_PATH_RELABELTYPE)
        {
            RelabelType *r = makeNode(RelabelType);
            r->arg = input;
            r->resulttype = dst_type;
            r->resulttypmod = dst_typmod;
            r->resultcollid = InvalidOid;
            r->relabelformat = COERCE_EXPLICIT_CAST;
            r->location = -1;
            return (Expr *)r;
        }
        else if (pathtype == COERCION_PATH_COERCEVIAIO)
        {
            Oid src_out, dst_in;
            getTypeOutputInfo(src_type, &src_out, (bool *)NULL);
            getTypeInputInfo(dst_type, &dst_in, (Oid *)NULL);

            CoerceViaIO *cio = makeNode(CoerceViaIO);
            cio->arg = input;
            cio->resulttype = dst_type;
            cio->resultcollid = InvalidOid;
            cio->coerceformat = COERCE_EXPLICIT_CAST;
            cio->location = -1;
            return (Expr *)cio;
        }

        ereport(ERROR,
                (errmsg("substrait: no coercion path from %u to %u",
                        src_type, dst_type)));
    }

    /* --- ScalarFunction → OpExpr or BoolExpr --- */
    if (expr.has_scalar_function())
    {
        const auto &sf = expr.scalar_function();
        auto fit = func_map.find(sf.function_reference());
        if (fit == func_map.end())
            ereport(ERROR,
                    (errmsg("substrait: unknown function anchor %u",
                            sf.function_reference())));

        const std::string &fname = fit->second;

        /* Boolean connectives */
        if (fname == "and" || fname == "or")
        {
            BoolExprType btype = (fname == "and") ? AND_EXPR : OR_EXPR;
            List *args = NIL;
            for (const auto &arg : sf.arguments())
            {
                if (!arg.has_value())
                    ereport(ERROR,
                            (errmsg("substrait: non-value bool arg")));
                args = lappend(args,
                               convert_expression(arg.value(),
                                                  schema, func_map,
                                                  virtuals, outer_schema));
            }
            BoolExpr *be = makeNode(BoolExpr);
            be->boolop = btype;
            be->args = args;
            be->location = -1;
            return (Expr *)be;
        }

        if (fname == "not")
        {
            if (sf.arguments_size() != 1 || !sf.arguments(0).has_value())
                ereport(ERROR,
                        (errmsg("substrait: bad NOT expression")));
            List *args = list_make1(
                convert_expression(sf.arguments(0).value(),
                                   schema, func_map, virtuals, outer_schema));
            BoolExpr *be = makeNode(BoolExpr);
            be->boolop = NOT_EXPR;
            be->args = args;
            be->location = -1;
            return (Expr *)be;
        }

        /* is_null / is_not_null */
        if (fname == "is_null" || fname == "is_not_null")
        {
            if (sf.arguments_size() != 1 || !sf.arguments(0).has_value())
                ereport(ERROR,
                        (errmsg("substrait: bad null-test expression")));
            NullTest *nt = makeNode(NullTest);
            nt->arg = convert_expression(sf.arguments(0).value(),
                                         schema, func_map, virtuals, outer_schema);
            nt->nulltesttype = (fname == "is_null") ? IS_NULL : IS_NOT_NULL;
            nt->argisrow = false;
            nt->location = -1;
            return (Expr *)nt;
        }

        /* like → PG ~~ operator (text ~~ text) */
        if (fname == "like")
        {
            if (sf.arguments_size() != 2)
                ereport(ERROR,
                        (errmsg("substrait: like expects 2 args")));
            Expr *left = coerce_to_text(convert_expression(
                sf.arguments(0).value(), schema, func_map, virtuals, outer_schema));
            Expr *right = coerce_to_text(convert_expression(
                sf.arguments(1).value(), schema, func_map, virtuals, outer_schema));

            List *opname = list_make1(makeString(pstrdup("~~")));
            Oid opoid = LookupOperName(NULL, opname, TEXTOID, TEXTOID,
                                       false, -1);
            HeapTuple optup = SearchSysCache1(OPEROID,
                                              ObjectIdGetDatum(opoid));
            Form_pg_operator opform = (Form_pg_operator)GETSTRUCT(optup);
            OpExpr *op = makeNode(OpExpr);
            op->opno = opoid;
            op->opfuncid = opform->oprcode;
            op->opresulttype = opform->oprresult;
            op->opretset = false;
            op->opcollid = InvalidOid;
            op->inputcollid = DEFAULT_COLLATION_OID;
            op->args = list_make2(left, right);
            op->location = -1;
            ReleaseSysCache(optup);
            return (Expr *)op;
        }

        /* extract → date_part(text, date/timestamp) */
        if (fname == "extract")
        {
            if (sf.arguments_size() != 2)
                ereport(ERROR,
                        (errmsg("substrait: extract expects 2 args")));

            /* arg0 = field (enum string like "YEAR"), arg1 = date value */
            const char *field_name = "year";
            const auto &arg0 = sf.arguments(0);
            if (arg0.has_enum_())
            {
                std::string e = arg0.enum_();
                /* Lowercase for PG date_part */
                for (auto &ch : e) ch = tolower(ch);
                field_name = pstrdup(e.c_str());
            }

            Expr *date_arg = convert_expression(sf.arguments(1).value(),
                                                schema, func_map, virtuals, outer_schema);
            Expr *field_text = (Expr *)makeConst(
                TEXTOID, -1, DEFAULT_COLLATION_OID, -1,
                PointerGetDatum(cstring_to_text(field_name)),
                false, false);

            Oid date_type = exprType((Node *)date_arg);
            Oid argtypes[2] = {TEXTOID, date_type};
            List *funcname = list_make1(makeString(pstrdup("date_part")));
            Oid funcoid = LookupFuncName(funcname, 2, argtypes, false);

            FuncExpr *fe = makeNode(FuncExpr);
            fe->funcid = funcoid;
            fe->funcresulttype = FLOAT8OID;
            fe->funcretset = false;
            fe->funcvariadic = false;
            fe->funcformat = COERCE_EXPLICIT_CALL;
            fe->funccollid = InvalidOid;
            fe->inputcollid = InvalidOid;
            fe->args = list_make2(field_text, date_arg);
            fe->location = -1;
            return (Expr *)fe;
        }

        /* substring(string, start, length) → PG substring(text,int,int) */
        if (fname == "substring")
        {
            if (sf.arguments_size() < 2)
                ereport(ERROR,
                        (errmsg("substrait: substring expects 2-3 args")));
            Expr *str_arg = coerce_to_text(convert_expression(
                sf.arguments(0).value(), schema, func_map,
                virtuals, outer_schema));
            Expr *start_arg = convert_expression(
                sf.arguments(1).value(), schema, func_map,
                virtuals, outer_schema);

            List *args = list_make2(str_arg, start_arg);
            int nargs = 2;
            Oid argtypes[3] = {TEXTOID, INT4OID, INT4OID};
            if (sf.arguments_size() >= 3)
            {
                Expr *len_arg = convert_expression(
                    sf.arguments(2).value(), schema, func_map,
                    virtuals, outer_schema);
                args = lappend(args, len_arg);
                nargs = 3;
            }

            List *funcname = list_make1(makeString(pstrdup("substring")));
            Oid funcoid = LookupFuncName(funcname, nargs, argtypes, false);

            FuncExpr *fe = makeNode(FuncExpr);
            fe->funcid = funcoid;
            fe->funcresulttype = TEXTOID;
            fe->funcretset = false;
            fe->funcvariadic = false;
            fe->funcformat = COERCE_EXPLICIT_CALL;
            fe->funccollid = DEFAULT_COLLATION_OID;
            fe->inputcollid = DEFAULT_COLLATION_OID;
            fe->args = args;
            fe->location = -1;
            return (Expr *)fe;
        }

        /* Generic PG function call by name (round, abs, coalesce, …). */
        static const std::unordered_map<std::string, const char *> kFuncNameMap = {
            {"round", "round"}, {"abs", "abs"}, {"coalesce", "coalesce"},
            {"upper", "upper"}, {"lower", "lower"}, {"char_length", "char_length"},
            {"concat", "concat"}, {"ltrim", "ltrim"}, {"rtrim", "rtrim"},
            {"trim", "btrim"}, {"replace", "replace"}, {"modulus", "mod"},
            {"negate", NULL},  /* handled as unary minus */
            {"power", "power"}, {"sqrt", "sqrt"}, {"ceil", "ceil"},
            {"floor", "floor"},
        };
        auto fnit = kFuncNameMap.find(fname);
        if (fnit != kFuncNameMap.end() && fnit->second != NULL)
        {
            List *args = NIL;
            std::vector<Oid> argtypes_v;
            for (int i = 0; i < sf.arguments_size(); i++)
            {
                Expr *a = convert_expression(sf.arguments(i).value(),
                                             schema, func_map,
                                             virtuals, outer_schema,
                                             window_query);
                args = lappend(args, a);
                argtypes_v.push_back(exprType((Node *)a));
            }
            List *funcname = list_make1(makeString(pstrdup(fnit->second)));
            Oid funcoid = LookupFuncName(funcname,
                                         (int)argtypes_v.size(),
                                         argtypes_v.data(), false);
            HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
            Oid rettype = ((Form_pg_proc)GETSTRUCT(proctup))->prorettype;
            ReleaseSysCache(proctup);

            FuncExpr *fe = makeNode(FuncExpr);
            fe->funcid = funcoid;
            fe->funcresulttype = rettype;
            fe->funcretset = false;
            fe->funcvariadic = false;
            fe->funcformat = COERCE_EXPLICIT_CALL;
            fe->funccollid = InvalidOid;
            fe->inputcollid = InvalidOid;
            fe->args = args;
            fe->location = -1;
            return (Expr *)fe;
        }

        /* negate → unary minus */
        if (fname == "negate")
        {
            Expr *arg = convert_expression(sf.arguments(0).value(), schema, func_map,
                                           virtuals, outer_schema);
            Oid argtype = exprType((Node *)arg);
            List *opname = list_make1(makeString(pstrdup("-")));
            Oid opoid = LookupOperName(NULL, opname, InvalidOid, argtype,
                                       false, -1);
            HeapTuple optup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opoid));
            Form_pg_operator opform = (Form_pg_operator)GETSTRUCT(optup);
            OpExpr *op = makeNode(OpExpr);
            op->opno = opoid;
            op->opfuncid = opform->oprcode;
            op->opresulttype = opform->oprresult;
            op->opretset = false;
            op->opcollid = InvalidOid;
            op->inputcollid = InvalidOid;
            op->args = list_make1(arg);
            op->location = -1;
            ReleaseSysCache(optup);
            return (Expr *)op;
        }

        /* Operator-based functions (comparison + arithmetic) */
        auto opit = kOpNameMap.find(fname);
        if (opit != kOpNameMap.end())
        {
            if (sf.arguments_size() != 2)
                ereport(ERROR,
                        (errmsg("substrait: operator %s expects 2 args, got %d",
                                fname.c_str(), sf.arguments_size())));

            Expr *left = convert_expression(sf.arguments(0).value(),
                                            schema, func_map,
                                            virtuals, outer_schema,
                                            window_query);
            Expr *right = convert_expression(sf.arguments(1).value(),
                                             schema, func_map,
                                             virtuals, outer_schema,
                                             window_query);

            Oid lefttype = exprType((Node *)left);
            Oid righttype = exprType((Node *)right);

            /* Coerce text/varchar/bpchar to a common type for operator lookup. */
            if (is_string_type(lefttype) && is_string_type(righttype) &&
                lefttype != righttype)
            {
                left = coerce_to_text(left);
                right = coerce_to_text(right);
                lefttype = TEXTOID;
                righttype = TEXTOID;
            }

            List *opname = list_make1(makeString(pstrdup(opit->second)));
            Oid opoid = LookupOperName(NULL, opname, lefttype, righttype,
                                       false, -1);

            HeapTuple optup = SearchSysCache1(OPEROID,
                                              ObjectIdGetDatum(opoid));
            if (!HeapTupleIsValid(optup))
                ereport(ERROR,
                        (errmsg("substrait: operator %s not found",
                                opit->second)));
            Form_pg_operator opform = (Form_pg_operator)GETSTRUCT(optup);
            Oid opfuncid = opform->oprcode;
            Oid oprestype = opform->oprresult;
            ReleaseSysCache(optup);

            Oid inputcollid = InvalidOid;
            if (is_string_type(lefttype) || is_string_type(righttype))
                inputcollid = DEFAULT_COLLATION_OID;

            OpExpr *op = makeNode(OpExpr);
            op->opno = opoid;
            op->opfuncid = opfuncid;
            op->opresulttype = oprestype;
            op->opretset = false;
            op->opcollid = InvalidOid;
            op->inputcollid = inputcollid;
            op->args = list_make2(left, right);
            op->location = -1;

            return (Expr *)op;
        }

        ereport(ERROR,
                (errmsg("substrait: unsupported scalar function \"%s\"",
                        fname.c_str())));
    }

    /* --- WindowFunction → WindowFunc --- */
    if (expr.has_window_function())
    {
        const auto &wf = expr.window_function();

        auto fit = func_map.find(wf.function_reference());
        if (fit == func_map.end())
            ereport(ERROR,
                    (errmsg("substrait: unknown window function anchor %u",
                            wf.function_reference())));

        const std::string &substrait_name = fit->second;
        auto nit = kAggNameMap.find(substrait_name);
        const std::string pg_name =
            (nit != kAggNameMap.end()) ? nit->second : substrait_name;

        /* Convert arguments. */
        List *args = NIL;
        std::vector<Oid> argtypes_v;
        bool is_star = (substrait_name == "count_star" ||
                        (pg_name == "count" && wf.arguments_size() == 0));
        if (!is_star)
        {
            for (const auto &arg : wf.arguments())
            {
                if (!arg.has_value()) continue;
                Expr *e = convert_expression(arg.value(), schema, func_map,
                                             virtuals, outer_schema, window_query);
                TargetEntry *tle = makeTargetEntry(e, (AttrNumber)(list_length(args) + 1),
                                                   NULL, false);
                args = lappend(args, tle);
                argtypes_v.push_back(exprType((Node *)e));
            }
        }

        /* Look up the function OID. */
        Oid funcoid;
        if (is_star || pg_name == "count")
            funcoid = lookup_count_star();
        else
        {
            List *funcname = list_make1(makeString(pstrdup(pg_name.c_str())));
            funcoid = LookupFuncName(funcname, (int)argtypes_v.size(),
                                     argtypes_v.data(), false);
        }

        HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
        Oid rettype = ((Form_pg_proc)GETSTRUCT(proctup))->prorettype;
        ReleaseSysCache(proctup);

        /* Build or find a matching WindowClause on the query. */
        Index winref = 0;
        if (window_query)
        {
            /* Build partition clause. */
            List *partitionClause = NIL;
            Index next_sgref = 0;
            ListCell *lc;
            foreach (lc, window_query->targetList)
            {
                TargetEntry *tle = (TargetEntry *)lfirst(lc);
                if (tle->ressortgroupref > next_sgref)
                    next_sgref = tle->ressortgroupref;
            }
            next_sgref++;

            for (const auto &pexpr : wf.partitions())
            {
                Expr *pe = convert_expression(pexpr, schema, func_map,
                                              virtuals, outer_schema, window_query);
                Oid ptype = exprType((Node *)pe);

                /* Find or add matching targetlist entry. */
                TargetEntry *match_tle = NULL;
                foreach (lc, window_query->targetList)
                {
                    TargetEntry *tle = (TargetEntry *)lfirst(lc);
                    if (equal(tle->expr, pe))
                    {
                        match_tle = tle;
                        break;
                    }
                }
                if (!match_tle)
                {
                    match_tle = makeTargetEntry(
                        pe,
                        (AttrNumber)(list_length(window_query->targetList) + 1),
                        pstrdup("?partition?"), true);
                    window_query->targetList = lappend(window_query->targetList,
                                                       match_tle);
                }
                if (match_tle->ressortgroupref == 0)
                    match_tle->ressortgroupref = next_sgref++;

                Oid eqop, sortop;
                bool hashable;
                get_sort_group_operators(ptype, false, true, false,
                                         &sortop, &eqop, NULL, &hashable);
                SortGroupClause *sgc = makeNode(SortGroupClause);
                sgc->tleSortGroupRef = match_tle->ressortgroupref;
                sgc->eqop = eqop;
                sgc->sortop = sortop;
                sgc->nulls_first = false;
                sgc->hashable = hashable;
                partitionClause = lappend(partitionClause, sgc);
            }

            /* Frame options. */
            int frameOptions = FRAMEOPTION_NONDEFAULT;
            auto bt = wf.bounds_type();
            /* 2 = RANGE, 1 = ROWS */
            if (bt == 2 || bt == 0)
                frameOptions |= FRAMEOPTION_RANGE;
            else
                frameOptions |= FRAMEOPTION_ROWS;

            if (wf.has_lower_bound())
            {
                auto &lb = wf.lower_bound();
                if (lb.has_unbounded())
                    frameOptions |= FRAMEOPTION_START_UNBOUNDED_PRECEDING;
                else if (lb.has_current_row())
                    frameOptions |= FRAMEOPTION_START_CURRENT_ROW;
                else
                    frameOptions |= FRAMEOPTION_START_UNBOUNDED_PRECEDING;
            }
            else
                frameOptions |= FRAMEOPTION_START_UNBOUNDED_PRECEDING;

            if (wf.has_upper_bound())
            {
                auto &ub = wf.upper_bound();
                if (ub.has_unbounded())
                    frameOptions |= FRAMEOPTION_END_UNBOUNDED_FOLLOWING;
                else if (ub.has_current_row())
                    frameOptions |= FRAMEOPTION_END_CURRENT_ROW;
                else
                    frameOptions |= FRAMEOPTION_END_UNBOUNDED_FOLLOWING;
            }
            else
                frameOptions |= FRAMEOPTION_END_CURRENT_ROW;

            WindowClause *wc = makeNode(WindowClause);
            wc->name = NULL;
            wc->refname = NULL;
            wc->partitionClause = partitionClause;
            wc->orderClause = NIL;
            wc->frameOptions = frameOptions;
            wc->startOffset = NULL;
            wc->endOffset = NULL;
            wc->startInRangeFunc = InvalidOid;
            wc->endInRangeFunc = InvalidOid;
            wc->inRangeColl = InvalidOid;
            wc->inRangeAsc = true;
            wc->inRangeNullsFirst = false;
            wc->copiedOrder = false;

            winref = (Index)(list_length(window_query->windowClause) + 1);
            wc->winref = winref;
            window_query->windowClause = lappend(window_query->windowClause, wc);
            window_query->hasWindowFuncs = true;
        }

        WindowFunc *wfunc = makeNode(WindowFunc);
        wfunc->winfnoid = funcoid;
        wfunc->wintype = rettype;
        wfunc->wincollid = InvalidOid;
        wfunc->inputcollid = InvalidOid;
        wfunc->args = args;
        wfunc->aggfilter = NULL;
        wfunc->winref = winref;
        wfunc->winstar = is_star;
        wfunc->winagg = true;
        wfunc->location = -1;

        return (Expr *)wfunc;
    }

    ereport(ERROR,
            (errmsg("substrait: unsupported expression type")));
    return NULL; /* unreachable */
}

/* ============================================================
 * SECTION 6 - SetRel (UNION / UNION ALL / INTERSECT / EXCEPT)
 * ============================================================ */

static Query *
build_set_query(const substrait::SetRel &setrel, const FuncMap &func_map,
                const SubstraitSchema *outer_schema)
{
    int ninputs = setrel.inputs_size();
    if (ninputs < 2)
        ereport(ERROR, (errmsg("substrait: SetRel needs >= 2 inputs")));

    /* Map Substrait SetOp to PG SetOperation + all flag. */
    SetOperation pg_op;
    bool all;
    switch (setrel.op())
    {
        case substrait::SetRel::SET_OP_UNION_ALL:
            pg_op = SETOP_UNION; all = true; break;
        case substrait::SetRel::SET_OP_UNION_DISTINCT:
            pg_op = SETOP_UNION; all = false; break;
        case substrait::SetRel::SET_OP_INTERSECTION_MULTISET:
        case substrait::SetRel::SET_OP_INTERSECTION_PRIMARY:
            pg_op = SETOP_INTERSECT; all = false; break;
        case substrait::SetRel::SET_OP_INTERSECTION_MULTISET_ALL:
            pg_op = SETOP_INTERSECT; all = true; break;
        case substrait::SetRel::SET_OP_MINUS_PRIMARY:
        case substrait::SetRel::SET_OP_MINUS_MULTISET:
            pg_op = SETOP_EXCEPT; all = false; break;
        case substrait::SetRel::SET_OP_MINUS_PRIMARY_ALL:
            pg_op = SETOP_EXCEPT; all = true; break;
        default:
            ereport(ERROR,
                    (errmsg("substrait: unsupported SetOp %d", (int)setrel.op())));
    }

    Query *query = makeNode(Query);
    query->commandType = CMD_SELECT;
    query->querySource = QSRC_ORIGINAL;
    query->canSetTag = true;
    query->rtable = NIL;
    query->rteperminfos = NIL;
    query->jointree = makeNode(FromExpr);
    query->jointree->fromlist = NIL;
    query->jointree->quals = NULL;

    /* Build each input as a subquery, add to rtable. */
    std::vector<int> rtindexes;
    for (int i = 0; i < ninputs; i++)
    {
        Query *subq = build_query_for_rel(
            &setrel.inputs(i), func_map, nullptr, outer_schema);

        RangeTblEntry *rte = makeNode(RangeTblEntry);
        rte->rtekind = RTE_SUBQUERY;
        rte->subquery = subq;
        rte->lateral = false;
        rte->inh = false;
        rte->inFromCl = true;

        List *colnames = NIL;
        ListCell *lc;
        foreach (lc, subq->targetList)
        {
            TargetEntry *tle = (TargetEntry *)lfirst(lc);
            colnames = lappend(colnames,
                makeString(pstrdup(tle->resname ? tle->resname : "?column?")));
        }
        char alias[16];
        snprintf(alias, sizeof(alias), "u%d", i);
        rte->eref = makeAlias(pstrdup(alias), colnames);

        query->rtable = lappend(query->rtable, rte);
        rtindexes.push_back(list_length(query->rtable));
    }

    /* Derive output column types from first input. */
    Query *first = ((RangeTblEntry *)linitial(query->rtable))->subquery;
    List *colTypes = NIL, *colTypmods = NIL, *colCollations = NIL;
    List *groupClauses = NIL;
    {
        ListCell *lc;
        Index sgref = 1;
        foreach (lc, first->targetList)
        {
            TargetEntry *tle = (TargetEntry *)lfirst(lc);
            Oid coltype = exprType((Node *)tle->expr);
            colTypes = lappend_oid(colTypes, coltype);
            colTypmods = lappend_int(colTypmods, -1);
            colCollations = lappend_oid(colCollations,
                is_string_type(coltype) ? DEFAULT_COLLATION_OID : InvalidOid);

            if (!all)
            {
                /* UNION DISTINCT needs groupClauses for dedup. */
                Oid ltop, eqop, gtop;
                bool hashable;
                get_sort_group_operators(coltype, false, true, false,
                                         &ltop, &eqop, &gtop, &hashable);
                SortGroupClause *sgc = makeNode(SortGroupClause);
                sgc->tleSortGroupRef = sgref++;
                sgc->eqop = eqop;
                sgc->sortop = ltop;
                sgc->nulls_first = false;
                sgc->hashable = hashable;
                groupClauses = lappend(groupClauses, sgc);
            }
        }
    }

    /* Build left-deep tree of SetOperationStmt. */
    auto make_leaf = [&](int idx) -> Node * {
        RangeTblRef *rtr = makeNode(RangeTblRef);
        rtr->rtindex = rtindexes[idx];
        return (Node *)rtr;
    };

    Node *tree = make_leaf(0);
    for (int i = 1; i < ninputs; i++)
    {
        SetOperationStmt *sos = makeNode(SetOperationStmt);
        sos->op = pg_op;
        sos->all = all;
        sos->larg = tree;
        sos->rarg = make_leaf(i);
        sos->colTypes = colTypes;
        sos->colTypmods = colTypmods;
        sos->colCollations = colCollations;
        sos->groupClauses = all ? NIL : groupClauses;
        tree = (Node *)sos;
    }
    query->setOperations = tree;

    /* Build target list referencing the output columns. */
    List *tlist = NIL;
    {
        AttrNumber resno = 1;
        ListCell *lc;
        Index sgref = 1;
        foreach (lc, first->targetList)
        {
            TargetEntry *tle = (TargetEntry *)lfirst(lc);
            Oid coltype = exprType((Node *)tle->expr);
            Oid collation = is_string_type(coltype)
                ? DEFAULT_COLLATION_OID : InvalidOid;
            Var *var = makeVar(0, resno, coltype, -1, collation, 0);
            TargetEntry *new_tle = makeTargetEntry(
                (Expr *)var, resno,
                pstrdup(tle->resname ? tle->resname : "?column?"),
                false);
            if (!all)
                new_tle->ressortgroupref = sgref++;
            tlist = lappend(tlist, new_tle);
            resno++;
        }
    }
    query->targetList = tlist;

    return query;
}

/* ============================================================
 * SECTION 7 - Top-level entry point
 *
 * build_query_for_rel: recursive core — builds a Query* from a Rel chain.
 * substrait_to_query: extern "C" entry — parses protobuf, delegates.
 * ============================================================ */

static Query *
build_query_for_rel(const substrait::Rel *cur, const FuncMap &func_map,
                    const substrait::RelRoot *root,
                    const SubstraitSchema *outer_schema)
{
    Query *query = makeNode(Query);
    query->commandType = CMD_SELECT;
    query->querySource = QSRC_ORIGINAL;
    query->canSetTag = true;
    query->rtable = NIL;
    query->rteperminfos = NIL;
    query->jointree = makeNode(FromExpr);
    query->jointree->fromlist = NIL;
    query->jointree->quals = NULL;

    /* Peel relations outermost-first.
     * [Fetch?] > [Sort?] > [Project?] > [Aggregate?] > [Project?] > [Filter?] > base */
    const substrait::FetchRel *fetch_rel = nullptr;
    if (cur->has_fetch())
    {
        fetch_rel = &cur->fetch();
        cur = &fetch_rel->input();
    }

    const substrait::SortRel *sort_rel = nullptr;
    if (cur->has_sort())
    {
        sort_rel = &cur->sort();
        cur = &sort_rel->input();
    }

    const substrait::ProjectRel *project_rel = nullptr;
    if (cur->has_project())
    {
        project_rel = &cur->project();
        cur = &project_rel->input();
    }

    const substrait::AggregateRel *agg_rel = nullptr;
    if (cur->has_aggregate())
    {
        agg_rel = &cur->aggregate();
        cur = &agg_rel->input();
    }

    /* Inner ProjectRel: between Aggregate and Read (e.g., TPC-H Q1). */
    const substrait::ProjectRel *inner_project_rel = nullptr;
    if (cur->has_project())
    {
        inner_project_rel = &cur->project();
        cur = &inner_project_rel->input();
    }

    const substrait::FilterRel *filter_rel = nullptr;
    if (cur->has_filter())
    {
        filter_rel = &cur->filter();
        cur = &filter_rel->input();
    }

    /* --- Base relation or subquery --- */
    SubstraitSchema schema;
    SubstraitSchema projected_schema;
    VirtualMap virtual_map;

    bool is_base = cur->has_read() || cur->has_cross() || cur->has_join();

    if (is_base)
    {
        VirtualMap base_virtuals;
        process_base_rel(*cur, query, schema, func_map, &base_virtuals);

        /* Apply filter */
        if (filter_rel != nullptr)
        {
            query->jointree->quals = (Node *)convert_expression(
                filter_rel->condition(), schema, func_map,
                base_virtuals, outer_schema);
        }
        else if (cur->has_read() && cur->read().has_filter())
        {
            query->jointree->quals = (Node *)convert_expression(
                cur->read().filter(), schema, func_map,
                base_virtuals, outer_schema);
        }

        /* Apply ReadRel projection */
        projected_schema = schema;
        if (cur->has_read())
        {
            const auto &read = cur->read();
            if (read.has_projection() &&
                read.projection().has_select())
            {
                const auto &sel = read.projection().select();
                projected_schema.clear();
                for (int i = 0; i < sel.struct_items_size(); i++)
                {
                    int base_idx = sel.struct_items(i).field();
                    if (base_idx >= 0 && base_idx < (int)schema.size())
                        projected_schema.push_back(schema[base_idx]);
                }
            }
        }

        /* Inner ProjectRel */
        if (inner_project_rel != nullptr)
        {
            std::vector<Expr *> raw_virtuals;
            for (int i = 0; i < inner_project_rel->expressions_size(); i++)
            {
                raw_virtuals.push_back(convert_expression(
                    inner_project_rel->expressions(i),
                    projected_schema, func_map, base_virtuals));
            }

            int n_input = (int)projected_schema.size();

            if (inner_project_rel->has_common() &&
                inner_project_rel->common().has_emit())
            {
                const auto &mapping =
                    inner_project_rel->common().emit().output_mapping();

                SubstraitSchema new_schema;
                for (int i = 0; i < mapping.size(); i++)
                {
                    int src = mapping.Get(i);
                    if (src < n_input)
                    {
                        new_schema.push_back(projected_schema[src]);
                        /* Propagate base virtuals through emit. */
                        auto bvit = base_virtuals.find(src);
                        if (bvit != base_virtuals.end())
                            virtual_map[i] = bvit->second;
                    }
                    else
                    {
                        Expr *ve = raw_virtuals[src - n_input];
                        new_schema.push_back({0, 0, exprType((Node *)ve)});
                        virtual_map[i] = ve;
                    }
                }
                projected_schema = new_schema;
            }
            else
            {
                /* Propagate base virtuals into virtual_map. */
                for (auto &[bvi, bve] : base_virtuals)
                    virtual_map[bvi] = bve;
                for (size_t i = 0; i < raw_virtuals.size(); i++)
                {
                    Expr *ve = raw_virtuals[i];
                    projected_schema.push_back({0, 0, exprType((Node *)ve)});
                    virtual_map[n_input + (int)i] = ve;
                }
            }
        }
        else if (!base_virtuals.empty())
        {
            /* No inner project, but base rel inlined virtuals. */
            virtual_map = base_virtuals;
        }
    }
    else
    {
        /* Nested rel chain or set operation — build as subquery. */
        Query *subq = cur->has_set()
            ? build_set_query(cur->set(), func_map, outer_schema)
            : build_query_for_rel(cur, func_map, nullptr, outer_schema);

        RangeTblEntry *sub_rte = makeNode(RangeTblEntry);
        sub_rte->rtekind = RTE_SUBQUERY;
        sub_rte->subquery = subq;
        sub_rte->lateral = false;
        sub_rte->inh = false;
        sub_rte->inFromCl = true;

        List *colnames = NIL;
        int attno = 1;
        ListCell *lc;
        foreach(lc, subq->targetList)
        {
            TargetEntry *tle = (TargetEntry *)lfirst(lc);
            colnames = lappend(colnames,
                makeString(pstrdup(tle->resname ? tle->resname : "?column?")));
            Oid t = exprType((Node *)tle->expr);
            projected_schema.push_back({0, (AttrNumber)attno, t});
            attno++;
        }
        sub_rte->eref = makeAlias(pstrdup("subq"), colnames);

        query->rtable = lappend(query->rtable, sub_rte);
        int sub_rtindex = list_length(query->rtable);

        for (auto &[rt, attnum, typeoid] : projected_schema)
            rt = sub_rtindex;
        schema = projected_schema;

        RangeTblRef *rtr = makeNode(RangeTblRef);
        rtr->rtindex = sub_rtindex;
        query->jointree->fromlist = lappend(query->jointree->fromlist, rtr);

        /* Apply inner_project on subquery output if present. */
        if (inner_project_rel != nullptr)
        {
            std::vector<Expr *> raw_virtuals;
            for (int i = 0; i < inner_project_rel->expressions_size(); i++)
            {
                raw_virtuals.push_back(convert_expression(
                    inner_project_rel->expressions(i),
                    projected_schema, func_map));
            }

            int n_input = (int)projected_schema.size();

            if (inner_project_rel->has_common() &&
                inner_project_rel->common().has_emit())
            {
                const auto &mapping =
                    inner_project_rel->common().emit().output_mapping();

                SubstraitSchema new_schema;
                for (int i = 0; i < mapping.size(); i++)
                {
                    int src = mapping.Get(i);
                    if (src < n_input)
                    {
                        new_schema.push_back(projected_schema[src]);
                    }
                    else
                    {
                        Expr *ve = raw_virtuals[src - n_input];
                        new_schema.push_back({0, 0, exprType((Node *)ve)});
                        virtual_map[i] = ve;
                    }
                }
                projected_schema = new_schema;
            }
            else
            {
                for (size_t i = 0; i < raw_virtuals.size(); i++)
                {
                    Expr *ve = raw_virtuals[i];
                    projected_schema.push_back({0, 0, exprType((Node *)ve)});
                    virtual_map[n_input + (int)i] = ve;
                }
            }
        }
    }

    /* Apply filter when base was a subquery (e.g. HAVING on aggregate subquery). */
    if (!is_base && filter_rel != nullptr && query->jointree->quals == NULL)
    {
        query->jointree->quals = (Node *)convert_expression(
            filter_rel->condition(), projected_schema, func_map,
            virtual_map, outer_schema);
    }

    /* --- Aggregate --- */
    SubstraitSchema project_input_schema = projected_schema;
    VirtualMap outer_virtuals;

    if (agg_rel != nullptr)
    {
        query->targetList = aggregate_rel_to_targetlist(
            *agg_rel, query, projected_schema, func_map, virtual_map);

        project_input_schema.clear();
        ListCell *lc;
        int idx = 0;
        foreach(lc, query->targetList)
        {
            TargetEntry *tle = (TargetEntry *)lfirst(lc);
            Oid otype = exprType((Node *)tle->expr);
            project_input_schema.push_back({0, 0, otype});
            outer_virtuals[idx++] = tle->expr;
        }
    }
    else if (!schema.empty())
    {
        /* No aggregate — build tlist from full schema (all tables).
         * Virtual columns (from inlined ProjectRel) use expressions
         * from virtual_map instead of Var nodes. */
        List *tlist = NIL;
        AttrNumber resno = 1;
        for (int si = 0; si < (int)schema.size(); si++)
        {
            auto &[rt, attnum, typeoid] = schema[si];
            Expr *expr;
            char *colname;
            auto vit = virtual_map.find(si);
            if (vit != virtual_map.end())
            {
                expr = (Expr *)copyObjectImpl(vit->second);
                colname = pstrdup("expr");
            }
            else
            {
                Oid collation = is_string_type(typeoid)
                    ? DEFAULT_COLLATION_OID : InvalidOid;
                expr = (Expr *)makeVar(rt, attnum, typeoid, -1, collation, 0);
                RangeTblEntry *rte =
                    (RangeTblEntry *)list_nth(query->rtable, rt - 1);
                colname = (rte->rtekind == RTE_RELATION)
                    ? get_attname(rte->relid, attnum, false)
                    : pstrdup("?column?");
            }
            TargetEntry *tle = makeTargetEntry(expr, resno++, colname, false);
            tlist = lappend(tlist, tle);
        }
        query->targetList = tlist;
    }

    /* --- Outer ProjectRel --- */
    if (project_rel != nullptr && project_rel->expressions_size() > 0)
    {
        int name_idx = list_length(query->targetList);

        for (int i = 0; i < project_rel->expressions_size(); i++)
        {
            Expr *expr = convert_expression(
                project_rel->expressions(i), project_input_schema,
                func_map, outer_virtuals, nullptr, query);

            const char *colname = "expr";
            if (root != nullptr && name_idx < root->names_size())
                colname = root->names(name_idx).c_str();

            TargetEntry *tle = makeTargetEntry(
                expr,
                (AttrNumber)(name_idx + 1),
                pstrdup(colname),
                false);
            query->targetList = lappend(query->targetList, tle);
            name_idx++;
        }

        if (project_rel->has_common() &&
            project_rel->common().has_emit())
        {
            const auto &emit = project_rel->common().emit();
            List *old_tlist = query->targetList;
            int old_len = list_length(old_tlist);

            std::unordered_map<Index, Expr *> sgref_expr;
            {
                ListCell *lc;
                foreach(lc, old_tlist)
                {
                    TargetEntry *tle = (TargetEntry *)lfirst(lc);
                    if (tle->ressortgroupref > 0)
                        sgref_expr[tle->ressortgroupref] = tle->expr;
                }
            }

            List *new_tlist = NIL;
            AttrNumber resno = 1;
            for (int i = 0; i < emit.output_mapping_size(); i++)
            {
                int src = emit.output_mapping(i);
                if (src < old_len)
                {
                    TargetEntry *tle = (TargetEntry *)
                        copyObjectImpl(list_nth(old_tlist, src));
                    tle->resno = resno++;
                    tle->resjunk = false;

                    for (auto &[ref, gexpr] : sgref_expr)
                    {
                        if (equal(tle->expr, gexpr))
                        {
                            tle->ressortgroupref = ref;
                            break;
                        }
                    }

                    new_tlist = lappend(new_tlist, tle);
                }
            }
            query->targetList = new_tlist;
        }
    }

    /* --- SortRel --- */
    if (sort_rel != nullptr)
    {
        Index next_sgref = 0;
        ListCell *lc;
        foreach (lc, query->targetList)
        {
            TargetEntry *tle = (TargetEntry *)lfirst(lc);
            if (tle->ressortgroupref > next_sgref)
                next_sgref = tle->ressortgroupref;
        }
        next_sgref++;

        for (int i = 0; i < sort_rel->sorts_size(); i++)
        {
            const auto &sf = sort_rel->sorts(i);

            if (!sf.has_expr() || !sf.expr().has_selection())
                ereport(ERROR,
                        (errmsg("substrait: non-field sort expression")));
            int field_idx = sf.expr().selection()
                                .direct_reference().struct_field().field();

            if (field_idx < 0 || field_idx >= list_length(query->targetList))
                ereport(ERROR,
                        (errmsg("substrait: sort field %d out of range",
                                field_idx)));
            TargetEntry *tle =
                (TargetEntry *)list_nth(query->targetList, field_idx);

            if (tle->ressortgroupref == 0)
                tle->ressortgroupref = next_sgref++;

            bool is_desc = false;
            bool nulls_first = false;
            if (sf.has_direction())
            {
                auto dir = sf.direction();
                is_desc = (dir == substrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST ||
                           dir == substrait::SortField::SORT_DIRECTION_DESC_NULLS_LAST);
                nulls_first = (dir == substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST ||
                               dir == substrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST);
            }

            Oid typeoid = exprType((Node *)tle->expr);
            Oid ltop, eqop, gtop;
            bool hashable;
            get_sort_group_operators(typeoid, !is_desc, true, is_desc,
                                     &ltop, &eqop, &gtop, &hashable);

            SortGroupClause *sgc = makeNode(SortGroupClause);
            sgc->tleSortGroupRef = tle->ressortgroupref;
            sgc->eqop = eqop;
            sgc->sortop = is_desc ? gtop : ltop;
            sgc->reverse_sort = is_desc;
            sgc->nulls_first = nulls_first;
            sgc->hashable = hashable;
            query->sortClause = lappend(query->sortClause, sgc);
        }
    }

    /* --- FetchRel → LIMIT / OFFSET --- */
    if (fetch_rel != nullptr)
    {
        int64_t count = -1, offset = 0;
        if (fetch_rel->has_count())
            count = fetch_rel->count();
        if (fetch_rel->has_offset())
            offset = fetch_rel->offset();

        if (count >= 0)
            query->limitCount = (Node *)makeConst(
                INT8OID, -1, InvalidOid, sizeof(int64),
                Int64GetDatum(count), false, true);
        if (offset > 0)
            query->limitOffset = (Node *)makeConst(
                INT8OID, -1, InvalidOid, sizeof(int64),
                Int64GetDatum(offset), false, true);
    }

    /* Flag SubLinks so the planner runs SS_process_sublinks. */
    if (contains_sublink(query->jointree->quals) ||
        contains_sublink((Node *)query->targetList))
        query->hasSubLinks = true;

    return query;
}

extern "C" Query *
substrait_to_query(const uint8_t *data, size_t len)
{
    substrait::Plan plan;
    if (!plan.ParseFromArray(data, (int)len))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("substrait: failed to deserialize protobuf Plan")));

    if (plan.relations_size() == 0)
        ereport(ERROR, (errmsg("substrait: plan contains no relations")));

    FuncMap func_map = build_func_map(plan);

    const substrait::PlanRel &prel = plan.relations(0);
    const substrait::Rel &root_rel =
        prel.has_root() ? prel.root().input() : prel.rel();

    const substrait::RelRoot *root =
        prel.has_root() ? &prel.root() : nullptr;

    return build_query_for_rel(&root_rel, func_map, root);
}
