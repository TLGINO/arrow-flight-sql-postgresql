/*
 * substrait_to_query.cpp
 *
 * Converts a serialized Substrait Plan into a PostgreSQL Query* tree.
 * Supports ReadRel + AggregateRel (sufficient for SELECT count(*) FROM t).
 *
 * Tested against PostgreSQL 18 + protobuf 3.x.
 *
 * INCLUDE ORDER IS CRITICAL:
 * Protobuf headers must come before PostgreSQL headers.
 * PostgreSQL's c.h defines ERROR=22 and other macros that corrupt
 * protobuf's internal logging tokens (LOGLEVEL_ERROR becomes LOGLEVEL_22).
 * We include protobuf first, then undef the conflicting PG macros,
 * then include PG headers.
 */

/* --- Step 1: C++ standard library (no conflicts) --- */
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>

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
#include "parser/parse_func.h"
#include "parser/parse_relation.h" /* addRTEPermissionInfo */
#include "parser/parse_oper.h"
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

typedef std::vector<std::pair<AttrNumber, Oid>> SubstraitSchema;

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
                    schema_out.push_back({attr->attnum, attr->atttypid});
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
            schema_out.push_back({attr->attnum, attr->atttypid});
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
                            int rtindex,
                            const SubstraitSchema &schema,
                            const FuncMap &func_map)
{
    query->hasAggs = true;

    List *tlist = NIL;
    AttrNumber resno = 1;

    RangeTblEntry *scan_rte =
        (RangeTblEntry *)list_nth(query->rtable, rtindex - 1);

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

            auto [attnum, typeoid] = schema[field_idx];

            Oid collation = InvalidOid;
            if (typeoid == TEXTOID || typeoid == VARCHAROID ||
                typeoid == BPCHAROID)
                collation = DEFAULT_COLLATION_OID;

            Var *var = makeVar(rtindex, attnum, typeoid, -1, collation, 0);

            Index sortgroupref = (Index)resno;
            char *attname = get_attname(scan_rte->relid, attnum, false);

            TargetEntry *tle = makeTargetEntry((Expr *)var, resno++,
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
                const auto &expr = arg.value();
                if (expr.has_selection())
                {
                    int idx = expr.selection()
                                  .direct_reference()
                                  .struct_field()
                                  .field();
                    if (idx < 0 || idx >= (int)schema.size())
                        ereport(ERROR,
                                (errmsg("substrait: arg field index %d out of range",
                                        idx)));
                    argtypes.push_back(schema[idx].second);
                }
                else if (expr.has_literal())
                {
                    argtypes.push_back(INT8OID);
                }
                else
                {
                    ereport(ERROR,
                            (errmsg("substrait: complex agg arg not supported")));
                }
            }
            List *funcname = list_make1(makeString(pstrdup(pg_agg.c_str())));
            aggfnoid = LookupFuncName(funcname, (int)argtypes.size(),
                                      argtypes.data(), false);
        }

        HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(aggfnoid));
        if (!HeapTupleIsValid(proctup))
            ereport(ERROR,
                    (errmsg("substrait: pg_proc entry missing for OID %u",
                            aggfnoid)));
        Oid rettype = ((Form_pg_proc)GETSTRUCT(proctup))->prorettype;
        ReleaseSysCache(proctup);

        List *agg_args = NIL;
        List *aggargtypes = NIL;
        if (!is_star)
        {
            for (const auto &arg : amf.arguments())
            {
                const auto &expr = arg.value();
                int idx = expr.selection()
                              .direct_reference()
                              .struct_field()
                              .field();
                auto [attnum, typeoid] = schema[idx];
                Var *var = makeVar(rtindex, attnum, typeoid, -1,
                                   InvalidOid, 0);
                TargetEntry *arg_tle =
                    makeTargetEntry((Expr *)var,
                                    (AttrNumber)(list_length(agg_args) + 1),
                                    NULL, false);
                agg_args = lappend(agg_args, arg_tle);
            }
            for (Oid oid : argtypes)
                aggargtypes = lappend_oid(aggargtypes, oid);
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
        aggref->aggdistinct = NIL;
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
 * SECTION 5 - Top-level entry point
 * ============================================================ */

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

    Query *query = makeNode(Query);
    query->commandType = CMD_SELECT;
    query->querySource = QSRC_ORIGINAL;
    query->canSetTag = true;
    query->rtable = NIL;
    query->rteperminfos = NIL;
    query->jointree = makeNode(FromExpr);
    query->jointree->fromlist = NIL;
    query->jointree->quals = NULL;

    const substrait::PlanRel &prel = plan.relations(0);
    const substrait::Rel &root_rel =
        prel.has_root() ? prel.root().input() : prel.rel();

    const substrait::Rel *cur = &root_rel;

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

    if (!cur->has_read())
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("substrait: unsupported plan shape - expected "
                        "[Project?] > [Aggregate?] > Read")));

    SubstraitSchema schema;
    RangeTblEntry *rte = read_rel_to_rte(cur->read(), schema, query);

    query->rtable = lappend(query->rtable, rte);
    int rtindex = list_length(query->rtable);

    RangeTblRef *rtr = makeNode(RangeTblRef);
    rtr->rtindex = rtindex;
    query->jointree->fromlist = lappend(query->jointree->fromlist, rtr);

    if (agg_rel != nullptr)
    {
        query->targetList = aggregate_rel_to_targetlist(
            *agg_rel, query, rtindex, schema, func_map);
    }
    else
    {
        query->targetList = build_star_tlist(rte, rtindex);
    }

    return query;
}
