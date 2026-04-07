/*
 * substrait_to_query.cpp
 *
 * Converts a serialized Substrait Plan into a PostgreSQL Query* tree.
 * Supported: [SortRel?] > [ProjectRel?] > [AggregateRel?] > [ProjectRel?] > [FilterRel?] > ReadRel|CrossRel
 * CrossRel trees (nested cross products) produce multi-table FROM clauses.
 * ReadRel.filter and ReadRel.projection are also handled.
 *
 * Tested against PostgreSQL 18 + protobuf 3.x.
 *
 * INCLUDE ORDER IS CRITICAL — protobuf headers before PG headers.
 * PG's c.h defines ERROR=22 etc. which corrupt protobuf's logging tokens.
 */

/* --- Step 1: C++ standard library (no conflicts) --- */
#include <algorithm>
#include <cstdarg>
#include <ctime>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
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
 * Exception used in place of ereport(ERROR) inside C++ code.
 * Caught at the extern "C" boundary after all C++ destructors run.
 * ============================================================ */
struct SubstraitError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

__attribute__((format(printf, 1, 2)))
static std::string sfmt(const char *fmt, ...)
{
    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    return std::string(buf);
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


/* (rtindex, attnum, typeoid, nullingrels) per field.
 * rtindex is 0 as placeholder in read_rel_to_rte; caller fixes up.
 * nullingrels tracks which join RTEs null this column (PG17 outer-join validation). */
typedef std::vector<std::tuple<int, AttrNumber, Oid, Bitmapset*>> SubstraitSchema;

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

/* Virtual expression map: field_index → precomputed PG Expr (forward decl). */
typedef std::unordered_map<int, Expr *> VirtualMap;

/* ============================================================
 * CTE dedup: detect identical Rel subtrees and emit PG CTEs
 * so PG materializes once instead of re-executing N times.
 * (Fixes Calcite CTE inlining, e.g. TPC-DS Q4.)
 * ============================================================ */
using FpHash = uint64_t;

struct RelCache {
    struct CteEntry {
        CommonTableExpr *cte;
        Query           *cte_query;
        List            *col_names;
        std::vector<Oid> col_types;
        std::vector<int32> col_typmods;
    };
    std::unordered_map<std::string, CteEntry> built;
    std::unordered_set<std::string> duplicates;
    /* O(N) hash-based dedup caches */
    std::unordered_map<const substrait::Rel*, FpHash> hash_cache;
    std::unordered_set<FpHash> dup_hashes;
    std::unordered_map<const substrait::Rel*, std::string> fp_cache;
    List *cte_list = NIL;
    int next_id = 0;
};

/* --- O(N) hash-based fingerprinting for CTE dedup --- */

static FpHash
hash_combine(FpHash h1, FpHash h2)
{
    return h1 ^ (h2 * 0x9e3779b97f4a7c15ULL + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
}

/* Recursively walk a Substrait Expression tree and collect Rel nodes
 * from any embedded subqueries (scalar, in_predicate, set_predicate).
 * This lets CTE dedup discover rels inside filter conditions, project
 * expressions, aggregate measures, and join conditions. */
static void
collect_expression_subquery_rels(const substrait::Expression &expr,
                                 std::vector<const substrait::Rel*> &out)
{
    if (expr.has_subquery())
    {
        const auto &sq = expr.subquery();
        if (sq.has_scalar())
            out.push_back(&sq.scalar().input());
        if (sq.has_in_predicate())
        {
            out.push_back(&sq.in_predicate().haystack());
            for (int i = 0; i < sq.in_predicate().needles_size(); i++)
                collect_expression_subquery_rels(sq.in_predicate().needles(i), out);
        }
        if (sq.has_set_predicate())
            out.push_back(&sq.set_predicate().tuples());
        return;
    }
    if (expr.has_scalar_function())
    {
        for (const auto &arg : expr.scalar_function().arguments())
            if (arg.has_value())
                collect_expression_subquery_rels(arg.value(), out);
        return;
    }
    if (expr.has_if_then())
    {
        const auto &ift = expr.if_then();
        for (int i = 0; i < ift.ifs_size(); i++)
        {
            if (ift.ifs(i).has_then())
                collect_expression_subquery_rels(ift.ifs(i).then(), out);
            if (ift.ifs(i).has_if_())
                collect_expression_subquery_rels(ift.ifs(i).if_(), out);
        }
        if (ift.has_else_())
            collect_expression_subquery_rels(ift.else_(), out);
        return;
    }
    if (expr.has_cast())
    {
        if (expr.cast().has_input())
            collect_expression_subquery_rels(expr.cast().input(), out);
        return;
    }
    if (expr.has_singular_or_list())
    {
        collect_expression_subquery_rels(expr.singular_or_list().value(), out);
        for (int i = 0; i < expr.singular_or_list().options_size(); i++)
            collect_expression_subquery_rels(expr.singular_or_list().options(i), out);
        return;
    }
    if (expr.has_window_function())
    {
        const auto &wf = expr.window_function();
        for (int i = 0; i < wf.arguments_size(); i++)
            if (wf.arguments(i).has_value())
                collect_expression_subquery_rels(wf.arguments(i).value(), out);
        for (int i = 0; i < wf.partitions_size(); i++)
            collect_expression_subquery_rels(wf.partitions(i), out);
        for (int i = 0; i < wf.sorts_size(); i++)
            if (wf.sorts(i).has_expr())
                collect_expression_subquery_rels(wf.sorts(i).expr(), out);
        return;
    }
    /* literal, selection, etc. — no sub-expressions */
}

static std::vector<const substrait::Rel*>
get_rel_children(const substrait::Rel &rel)
{
    std::vector<const substrait::Rel*> out;
    if (rel.has_fetch())    out.push_back(&rel.fetch().input());
    if (rel.has_sort())     out.push_back(&rel.sort().input());
    if (rel.has_project())  out.push_back(&rel.project().input());
    if (rel.has_aggregate()) out.push_back(&rel.aggregate().input());
    if (rel.has_filter())   out.push_back(&rel.filter().input());
    if (rel.has_cross())
    {
        out.push_back(&rel.cross().left());
        out.push_back(&rel.cross().right());
    }
    if (rel.has_join())
    {
        out.push_back(&rel.join().left());
        out.push_back(&rel.join().right());
    }
    if (rel.has_set())
    {
        for (int i = 0; i < rel.set().inputs_size(); i++)
            out.push_back(&rel.set().inputs(i));
    }

    /* Walk expressions to find rels inside subqueries. */
    if (rel.has_filter() && rel.filter().has_condition())
        collect_expression_subquery_rels(rel.filter().condition(), out);
    if (rel.has_project())
        for (int i = 0; i < rel.project().expressions_size(); i++)
            collect_expression_subquery_rels(rel.project().expressions(i), out);
    if (rel.has_aggregate())
        for (const auto &m : rel.aggregate().measures())
            for (const auto &arg : m.measure().arguments())
                if (arg.has_value())
                    collect_expression_subquery_rels(arg.value(), out);
    if (rel.has_join() && rel.join().has_expression())
        collect_expression_subquery_rels(rel.join().expression(), out);
    /* CrossRel has no expression field in this protobuf version. */

    return out;
}

/* Fast structural hash (O(N) over the tree). */
static FpHash
fingerprint_rel_hash(const substrait::Rel &rel,
                     std::unordered_map<const substrait::Rel*, FpHash> &cache)
{
    auto it = cache.find(&rel);
    if (it != cache.end()) return it->second;
    FpHash h = std::hash<int>{}(rel.rel_type_case());
    h = hash_combine(h, std::hash<size_t>{}(rel.ByteSizeLong()));
    for (auto *child : get_rel_children(rel))
        h = hash_combine(h, fingerprint_rel_hash(*child, cache));
    cache[&rel] = h;
    return h;
}

/* Exact fingerprint (serialization), cached per Rel pointer. */
static const std::string &
fingerprint_rel(const substrait::Rel &rel, RelCache &rc)
{
    auto it = rc.fp_cache.find(&rel);
    if (it != rc.fp_cache.end()) return it->second;
    auto &s = rc.fp_cache[&rel];
    s = rel.SerializeAsString();
    return s;
}

/* Two-phase dedup: O(N) hash pass, then exact serialization only on collisions. */
static void
count_rel_subtrees(const substrait::Rel &rel, RelCache &rel_cache)
{
    /* Phase 1: collect all nodes and compute hashes. */
    std::vector<const substrait::Rel*> all_nodes;
    std::function<void(const substrait::Rel &)> collect =
        [&](const substrait::Rel &r)
    {
        all_nodes.push_back(&r);
        for (auto *child : get_rel_children(r))
            collect(*child);
    };
    collect(rel);

    /* Count hashes. */
    std::unordered_map<FpHash, int> hash_counts;
    for (auto *node : all_nodes)
    {
        FpHash h = fingerprint_rel_hash(*node, rel_cache.hash_cache);
        hash_counts[h]++;
    }

    /* Record which hashes appear >=2 times. */
    for (auto &[h, cnt] : hash_counts)
    {
        if (cnt >= 2)
            rel_cache.dup_hashes.insert(h);
    }

    /* Phase 2: exact serialization only for candidate duplicates. */
    std::unordered_map<std::string, int> exact_counts;
    for (auto *node : all_nodes)
    {
        FpHash h = rel_cache.hash_cache[node];
        if (!rel_cache.dup_hashes.count(h))
            continue;
        const std::string &fp = fingerprint_rel(*node, rel_cache);
        exact_counts[fp]++;
    }

    for (auto &[fp, cnt] : exact_counts)
    {
        if (cnt >= 2 && fp.size() > 256)
        {
            /* Skip dedup for CrossRel/JoinRel: materializing them as CTEs
             * creates unfiltered Cartesian products (e.g. TPC-DS Q88). */
            substrait::Rel tmp;
            tmp.ParseFromString(fp);
            if (tmp.has_cross() || tmp.has_join())
                continue;
            rel_cache.duplicates.insert(fp);
        }
    }
}

/* Build a simple SELECT * FROM cte_name query for a CTE reference. */
static Query *
build_cte_ref_query(const RelCache::CteEntry &entry, int depth)
{
    Query *q = makeNode(Query);
    q->commandType = CMD_SELECT;
    q->querySource = QSRC_ORIGINAL;
    q->canSetTag = true;
    q->rtable = NIL;
    q->rteperminfos = NIL;
    q->jointree = makeNode(FromExpr);
    q->jointree->fromlist = NIL;
    q->jointree->quals = NULL;

    /* RTE_CTE entry */
    RangeTblEntry *rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_CTE;
    rte->ctename = pstrdup(entry.cte->ctename);
    rte->ctelevelsup = depth;
    rte->lateral = false;
    rte->inh = false;
    rte->inFromCl = true;
    rte->eref = makeAlias(pstrdup(entry.cte->ctename),
                           (List *)copyObjectImpl(entry.col_names));
    rte->coltypes = NIL;
    rte->coltypmods = NIL;
    rte->colcollations = NIL;
    for (size_t i = 0; i < entry.col_types.size(); i++)
    {
        rte->coltypes = lappend_oid(rte->coltypes, entry.col_types[i]);
        rte->coltypmods = lappend_int(rte->coltypmods, entry.col_typmods[i]);
        rte->colcollations = lappend_oid(rte->colcollations,
            is_string_type(entry.col_types[i])
                ? DEFAULT_COLLATION_OID : InvalidOid);
    }

    q->rtable = lappend(q->rtable, rte);

    RangeTblRef *rtr = makeNode(RangeTblRef);
    rtr->rtindex = 1;
    q->jointree->fromlist = lappend(q->jointree->fromlist, rtr);

    /* Target list: SELECT col1, col2, ... */
    AttrNumber resno = 1;
    ListCell *lc;
    foreach(lc, entry.col_names)
    {
        Oid t = entry.col_types[resno - 1];
        int32 tm = entry.col_typmods[resno - 1];
        Oid coll = is_string_type(t) ? DEFAULT_COLLATION_OID : InvalidOid;
        Var *v = makeVar(1, resno, t, tm, coll, 0);
        TargetEntry *tle = makeTargetEntry(
            (Expr *)v, resno,
            pstrdup(strVal(lfirst(lc))),
            false);
        q->targetList = lappend(q->targetList, tle);
        resno++;
    }

    return q;
}

/* Forward declaration (needed by virtual_table handling in read_rel_to_rte). */
static Expr *
convert_expression(const substrait::Expression &expr,
                   const SubstraitSchema &schema,
                   const FuncMap &func_map,
                   const VirtualMap &virtuals = {},
                   const SubstraitSchema *outer_schema = nullptr,
                   RelCache *rel_cache = nullptr, int depth = 0);

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
    /* virtual_table → RTE_VALUES (inline literal rows, e.g. VALUES (...)) */
    if (read.has_virtual_table())
    {
        const auto &vt = read.virtual_table();
        if (vt.expressions_size() == 0)
            throw SubstraitError("substrait: virtual_table has no rows");

        SubstraitSchema empty_schema;
        FuncMap empty_fm;
        List *values_lists = NIL;
        List *colTypes = NIL, *colTypmods = NIL, *colCollations = NIL;
        int ncols = 0;

        for (int r = 0; r < vt.expressions_size(); r++)
        {
            const auto &row = vt.expressions(r);
            List *row_exprs = NIL;
            for (int c = 0; c < row.fields_size(); c++)
            {
                Expr *val = convert_expression(
                    row.fields(c), empty_schema, empty_fm);
                row_exprs = lappend(row_exprs, val);

                if (r == 0)
                {
                    Oid t = exprType((Node *)val);
                    Oid coll = is_string_type(t)
                        ? DEFAULT_COLLATION_OID : InvalidOid;
                    colTypes = lappend_oid(colTypes, t);
                    colTypmods = lappend_int(colTypmods, -1);
                    colCollations = lappend_oid(colCollations, coll);
                    schema_out.push_back({0, (AttrNumber)(c + 1), t, nullptr});
                    ncols++;
                }
            }
            values_lists = lappend(values_lists, row_exprs);
        }

        /* Column names from base_schema if available. */
        List *colnames = NIL;
        if (read.has_base_schema())
        {
            for (int i = 0; i < read.base_schema().names_size() && i < ncols; i++)
            {
                std::string nm = read.base_schema().names(i);
                std::transform(nm.begin(), nm.end(), nm.begin(), ::tolower);
                colnames = lappend(colnames, makeString(pstrdup(nm.c_str())));
            }
        }
        for (int i = list_length(colnames); i < ncols; i++)
            colnames = lappend(colnames,
                makeString(psprintf("col%d", i + 1)));

        RangeTblEntry *rte = makeNode(RangeTblEntry);
        rte->rtekind = RTE_VALUES;
        rte->values_lists = values_lists;
        rte->coltypes = colTypes;
        rte->coltypmods = colTypmods;
        rte->colcollations = colCollations;
        rte->lateral = false;
        rte->inh = false;
        rte->inFromCl = true;
        rte->alias = NULL;
        rte->eref = makeAlias(pstrdup("*VALUES*"), colnames);

        return rte;
    }

    if (!read.has_named_table())
        throw SubstraitError("substrait: only named-table reads are supported");

    const auto &names = read.named_table().names();
    if (names.empty())
        throw SubstraitError("substrait: named table has no names");

    List *relname = NIL;
    for (const auto &n : names)
    {
        std::string lower = n;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        relname = lappend(relname, makeString(pstrdup(lower.c_str())));
    }

    RangeVar *rv = makeRangeVarFromNameList(relname);
    Oid reloid = RangeVarGetRelid(rv, AccessShareLock, false);

    Relation rel = RelationIdGetRelation(reloid);
    if (!RelationIsValid(rel))
        throw SubstraitError(sfmt("substrait: could not open relation %u", reloid));

    TupleDesc tupdesc = RelationGetDescr(rel);
    List *colnames = NIL;

    if (read.has_base_schema())
    {
        const auto &bs = read.base_schema();

        /* Build lowercase attname → index map for O(1) lookup */
        std::unordered_map<std::string, int> att_map;
        for (int j = 0; j < tupdesc->natts; j++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, j);
            if (attr->attisdropped)
                continue;
            std::string lower = NameStr(attr->attname);
            std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
            att_map[lower] = j;
        }

        for (int i = 0; i < bs.names_size(); i++)
        {
            std::string col_lower = bs.names(i);
            std::transform(col_lower.begin(), col_lower.end(),
                           col_lower.begin(), ::tolower);
            auto it = att_map.find(col_lower);
            if (it == att_map.end())
            {
                RelationClose(rel);
                throw SubstraitError(sfmt("substrait: column \"%s\" not found in \"%s\"",
                                          bs.names(i).c_str(), names[0].c_str()));
            }
            Form_pg_attribute attr = TupleDescAttr(tupdesc, it->second);
            schema_out.push_back({0, attr->attnum, attr->atttypid, nullptr});
            colnames = lappend(colnames,
                               makeString(pstrdup(NameStr(attr->attname))));
        }
    }
    else
    {
        for (int j = 0; j < tupdesc->natts; j++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, j);
            if (attr->attisdropped)
                continue;
            schema_out.push_back({0, attr->attnum, attr->atttypid, nullptr});
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

static Query *
build_query_for_rel(const substrait::Rel *cur, const FuncMap &func_map,
                    const substrait::RelRoot *root,
                    const SubstraitSchema *outer_schema = nullptr,
                    RelCache *rel_cache = nullptr, int depth = 0);

/* Build a from-list Node for a base relation tree.
 * Adds RTEs to rtable. Returns a single Node* for Read/Join;
 * for Cross, adds children to fromlist directly and returns NULL. */
static Node *
build_from_node(const substrait::Rel &rel, Query *query,
                SubstraitSchema &schema_out, const FuncMap &func_map,
                RelCache *rel_cache = nullptr, int depth = 0,
                bool flatten_crosses = true)
{
    if (rel.has_read())
    {
        SubstraitSchema local;
        RangeTblEntry *rte = read_rel_to_rte(rel.read(), local, query);
        query->rtable = lappend(query->rtable, rte);
        int rtindex = list_length(query->rtable);

        for (auto &[rt, attnum, typeoid, nullingrels] : local)
            rt = rtindex;
        schema_out.insert(schema_out.end(), local.begin(), local.end());

        RangeTblRef *rtr = makeNode(RangeTblRef);
        rtr->rtindex = rtindex;
        return (Node *)rtr;
    }
    else if (rel.has_cross())
    {
        const auto &cross = rel.cross();
        SubstraitSchema left_schema, right_schema;

        Node *larg = build_from_node(cross.left(), query, left_schema,
                                     func_map, rel_cache, depth,
                                     flatten_crosses);
        Node *rarg = build_from_node(cross.right(), query, right_schema,
                                     func_map, rel_cache, depth,
                                     flatten_crosses);

        SubstraitSchema combined = left_schema;
        combined.insert(combined.end(), right_schema.begin(),
                        right_schema.end());

        if (flatten_crosses)
        {
            /*
             * Flatten: add children directly to fromlist instead of creating
             * nested JoinExpr. For CROSS(CROSS(A,B),C) this yields
             * fromlist=[A,B,C], letting PG's join_collapse_limit optimize
             * the join order natively.
             */
            if (larg)
                query->jointree->fromlist = lappend(query->jointree->fromlist, larg);
            if (rarg)
                query->jointree->fromlist = lappend(query->jointree->fromlist, rarg);

            schema_out = combined;
            return NULL;  /* signals items added to fromlist directly */
        }
        else
        {
            /* Non-flattened: return a JoinExpr so parent JoinRel gets
             * a valid larg/rarg node. */
            List *alias_vars = NIL;
            List *col_names = NIL;
            for (auto &[rt, attnum, typeoid, nullingrels] : combined)
            {
                Oid collation = is_string_type(typeoid)
                    ? DEFAULT_COLLATION_OID : InvalidOid;
                Var *v = makeVar(rt, attnum, typeoid, -1, collation, 0);
                if (nullingrels)
                    v->varnullingrels = bms_copy(nullingrels);
                alias_vars = lappend(alias_vars, (Node *)v);
                col_names = lappend(col_names,
                    makeString(pstrdup("?column?")));
            }

            RangeTblEntry *join_rte = makeNode(RangeTblEntry);
            join_rte->rtekind = RTE_JOIN;
            join_rte->jointype = JOIN_INNER;
            join_rte->joinmergedcols = 0;
            join_rte->joinaliasvars = alias_vars;
            join_rte->joinleftcols = NIL;
            join_rte->joinrightcols = NIL;
            join_rte->join_using_alias = NULL;
            join_rte->eref = makeAlias(pstrdup("unnamed_cross"), col_names);
            join_rte->lateral = false;
            join_rte->inh = false;
            join_rte->inFromCl = true;

            query->rtable = lappend(query->rtable, join_rte);
            int join_rtindex = list_length(query->rtable);

            JoinExpr *jexpr = makeNode(JoinExpr);
            jexpr->jointype = JOIN_INNER;
            jexpr->isNatural = false;
            jexpr->larg = larg;
            jexpr->rarg = rarg;
            jexpr->usingClause = NIL;
            jexpr->join_using_alias = NULL;
            jexpr->quals = NULL;
            jexpr->alias = NULL;
            jexpr->rtindex = join_rtindex;

            schema_out = combined;
            return (Node *)jexpr;
        }
    }
    else if (rel.has_join())
    {
        const auto &jn = rel.join();

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
                throw SubstraitError(sfmt("substrait: unsupported join type %d"
                                          " (SEMI/ANTI joins require EXISTS subquery conversion)",
                                          jn.type()));
        }

        /*
         * INNER JOIN flattening: add children to FROM-list and move ON
         * conditions to WHERE, just like CrossRel flattening. This gives
         * PG's optimizer full join reordering freedom (controlled by
         * join_collapse_limit). Only safe for INNER joins — outer joins
         * must preserve their structure.
         */
        if (flatten_crosses && pg_jtype == JOIN_INNER)
        {
            SubstraitSchema left_schema, right_schema;

            Node *larg = build_from_node(jn.left(), query, left_schema,
                                         func_map, rel_cache, depth,
                                         /*flatten_crosses=*/true);
            Node *rarg = build_from_node(jn.right(), query, right_schema,
                                         func_map, rel_cache, depth,
                                         /*flatten_crosses=*/true);

            SubstraitSchema combined = left_schema;
            combined.insert(combined.end(), right_schema.begin(),
                            right_schema.end());

            /* Move ON condition to WHERE clause. */
            if (jn.has_expression())
            {
                Node *on_quals = (Node *)convert_expression(
                    jn.expression(), combined, func_map,
                    {}, nullptr, rel_cache, depth);
                if (query->jointree->quals == NULL)
                    query->jointree->quals = on_quals;
                else
                {
                    BoolExpr *and_expr = makeNode(BoolExpr);
                    and_expr->boolop = AND_EXPR;
                    and_expr->args = list_make2(query->jointree->quals,
                                                on_quals);
                    and_expr->location = -1;
                    query->jointree->quals = (Node *)and_expr;
                }
            }

            /* Add children to FROM-list (same as cross-join flattening). */
            if (larg)
                query->jointree->fromlist =
                    lappend(query->jointree->fromlist, larg);
            if (rarg)
                query->jointree->fromlist =
                    lappend(query->jointree->fromlist, rarg);

            schema_out = combined;
            return NULL;  /* signals items added to fromlist directly */
        }

        SubstraitSchema left_schema, right_schema;

        Node *larg = build_from_node(jn.left(), query, left_schema, func_map,
                                     rel_cache, depth,
                                     /*flatten_crosses=*/false);
        Node *rarg = build_from_node(jn.right(), query, right_schema, func_map,
                                     rel_cache, depth,
                                     /*flatten_crosses=*/false);

        /* Combined schema for join condition resolution. */
        SubstraitSchema combined = left_schema;
        combined.insert(combined.end(), right_schema.begin(),
                        right_schema.end());

        /* RTE_JOIN entry for the planner. */
        int join_rtindex = list_length(query->rtable) + 1;
        int left_count = (int)left_schema.size();

        /* Build quals FIRST — ON-clause Vars must NOT carry nullingrels.
         * PG's pull_varnos() adds varnullingrels to relids, which would
         * fail the ojscope subset check in distribute_qual_to_rels(). */
        Node *quals = NULL;
        if (jn.has_expression())
            quals = (Node *)convert_expression(jn.expression(),
                                               combined, func_map,
                                               {}, nullptr, rel_cache, depth);

        /* NOW stamp nullingrels on nullable-side schema entries.
         * These propagate into alias_vars and schema_out for upper levels. */
        for (int i = 0; i < (int)combined.size(); i++)
        {
            auto &[rt, attnum, typeoid, nr] = combined[i];
            bool from_right = (i >= left_count);
            bool nullable = (pg_jtype == JOIN_LEFT && from_right)
                         || (pg_jtype == JOIN_RIGHT && !from_right)
                         || (pg_jtype == JOIN_FULL);
            if (nullable)
                nr = bms_add_member(bms_copy(nr), join_rtindex);
        }

        List *alias_vars = NIL;
        List *col_names = NIL;
        for (auto &[rt, attnum, typeoid, nullingrels] : combined)
        {
            Oid collation = is_string_type(typeoid)
                ? DEFAULT_COLLATION_OID : InvalidOid;
            Var *v = makeVar(rt, attnum, typeoid, -1, collation, 0);
            if (nullingrels)
                v->varnullingrels = bms_copy(nullingrels);
            alias_vars = lappend(alias_vars, v);
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
        /* join_rtindex already computed above */

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

    /* Non-base rel (e.g. Aggregate inside a Cross) — wrap as subquery. */
    Query *subq = build_query_for_rel(&rel, func_map, nullptr, nullptr,
                                      rel_cache, depth + 1);

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
        if (tle->resjunk)
            continue;
        colnames = lappend(colnames,
            makeString(pstrdup(tle->resname ? tle->resname : "?column?")));
        Oid t = exprType((Node *)tle->expr);
        schema_out.push_back({0, (AttrNumber)attno, t, nullptr});
        attno++;
    }
    sub_rte->eref = makeAlias(pstrdup("subq"), colnames);

    query->rtable = lappend(query->rtable, sub_rte);
    int sub_rtindex = list_length(query->rtable);

    for (auto &[rt, attnum, typeoid, nullingrels] : schema_out)
        if (rt == 0) rt = sub_rtindex;

    RangeTblRef *rtr = makeNode(RangeTblRef);
    rtr->rtindex = sub_rtindex;
    return (Node *)rtr;
}

/* Top-level: process base relation tree into query->jointree->fromlist. */
static void
process_base_rel(const substrait::Rel &rel, Query *query,
                 SubstraitSchema &schema_out, const FuncMap &func_map,
                 RelCache *rel_cache = nullptr, int depth = 0)
{
    Node *node = build_from_node(rel, query, schema_out, func_map,
                                 rel_cache, depth);
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

/* Pure window functions (not aggregates). Maps Substrait name → PG name. */
static const std::unordered_map<std::string, std::string> kWinFuncNameMap = {
    {"rank", "rank"},
    {"dense_rank", "dense_rank"},
    {"row_number", "row_number"},
    {"ntile", "ntile"},
    {"lag", "lag"},
    {"lead", "lead"},
    {"first_value", "first_value"},
    {"last_value", "last_value"},
    {"nth_value", "nth_value"},
    {"percent_rank", "percent_rank"},
    {"cume_dist", "cume_dist"},
};

/* -------- Window function deferred-spec infrastructure --------
 *
 * convert_expression returns a WindowFunc* node but cannot create the
 * WindowClause (it has no access to Query*).  We stash the partition /
 * sort / frame info per WindowFunc* in a file-static map.  After the
 * target list is built, fixup_window_clauses() groups identical specs
 * into WindowClause entries and sets winref on every WindowFunc.
 *
 * Safe because PG backends are single-threaded.
 */

struct PendingWinSpec
{
    List       *partition_exprs;   /* List of Expr* */
    List       *sort_exprs;        /* List of Expr* (sort keys) */
    List       *sort_dirs;         /* List of int   (SortField::Direction) */
    int         frame_options;
    Node       *start_offset;
    Node       *end_offset;
};

static std::unordered_map<WindowFunc *, PendingWinSpec> g_pending_wins;

/*
 * Map Substrait WindowFunction bounds to PG frameOptions.
 */
static int
substrait_bounds_to_frame(
    const substrait::Expression_WindowFunction &wf,
    Node **start_offset, Node **end_offset)
{
    using Bound = substrait::Expression_WindowFunction_Bound;
    int opts = FRAMEOPTION_NONDEFAULT | FRAMEOPTION_BETWEEN;
    *start_offset = NULL;
    *end_offset = NULL;

    /* ROWS vs RANGE (default RANGE). */
    if (wf.bounds_type() == substrait::Expression_WindowFunction_BoundsType_BOUNDS_TYPE_ROWS)
        opts |= FRAMEOPTION_ROWS;
    else
        opts |= FRAMEOPTION_RANGE;

    /* Lower bound */
    if (wf.has_lower_bound())
    {
        const auto &lb = wf.lower_bound();
        if (lb.has_unbounded())
            opts |= FRAMEOPTION_START_UNBOUNDED_PRECEDING;
        else if (lb.has_current_row())
            opts |= FRAMEOPTION_START_CURRENT_ROW;
        else if (lb.has_preceding())
        {
            opts |= FRAMEOPTION_START_OFFSET_PRECEDING;
            *start_offset = (Node *)makeConst(
                INT8OID, -1, InvalidOid, sizeof(int64),
                Int64GetDatum(lb.preceding().offset()), false, true);
        }
        else if (lb.has_following())
        {
            opts |= FRAMEOPTION_START_OFFSET_FOLLOWING;
            *start_offset = (Node *)makeConst(
                INT8OID, -1, InvalidOid, sizeof(int64),
                Int64GetDatum(lb.following().offset()), false, true);
        }
    }
    else
        opts |= FRAMEOPTION_START_UNBOUNDED_PRECEDING;

    /* Upper bound */
    if (wf.has_upper_bound())
    {
        const auto &ub = wf.upper_bound();
        if (ub.has_unbounded())
            opts |= FRAMEOPTION_END_UNBOUNDED_FOLLOWING;
        else if (ub.has_current_row())
            opts |= FRAMEOPTION_END_CURRENT_ROW;
        else if (ub.has_preceding())
        {
            opts |= FRAMEOPTION_END_OFFSET_PRECEDING;
            *end_offset = (Node *)makeConst(
                INT8OID, -1, InvalidOid, sizeof(int64),
                Int64GetDatum(ub.preceding().offset()), false, true);
        }
        else if (ub.has_following())
        {
            opts |= FRAMEOPTION_END_OFFSET_FOLLOWING;
            *end_offset = (Node *)makeConst(
                INT8OID, -1, InvalidOid, sizeof(int64),
                Int64GetDatum(ub.following().offset()), false, true);
        }
    }
    else
        opts |= FRAMEOPTION_END_CURRENT_ROW;

    return opts;
}

/*
 * Walk expression tree collecting all WindowFunc nodes.
 */
static bool
collect_window_funcs_walker(Node *node, List **wfuncs)
{
    if (node == NULL)
        return false;
    if (IsA(node, WindowFunc))
    {
        *wfuncs = lappend(*wfuncs, node);
        /* still recurse into args */
    }
    if (IsA(node, Query))
        return false; /* don't descend into sub-Queries */
    return expression_tree_walker(node,
        (bool (*)()) collect_window_funcs_walker, wfuncs);
}

/*
 * After building the target list, create WindowClause entries for all
 * WindowFunc nodes found in the tlist, grouping identical specs.
 */
static void
fixup_window_clauses(Query *query)
{
    /* Collect all WindowFunc nodes from the target list. */
    List *wfuncs = NIL;
    ListCell *lc;
    foreach(lc, query->targetList)
    {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        collect_window_funcs_walker((Node *)tle->expr, &wfuncs);
    }

    if (wfuncs == NIL)
        return;

    query->hasWindowFuncs = true;

    /* Group into WindowClauses.  Simple O(n^2) — n is tiny. */
    Index next_winref = 1;
    Index next_sgref = 0;
    foreach(lc, query->targetList)
    {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (tle->ressortgroupref > next_sgref)
            next_sgref = tle->ressortgroupref;
    }
    next_sgref++;

    ListCell *wlc;
    foreach(wlc, wfuncs)
    {
        WindowFunc *wf = (WindowFunc *)lfirst(wlc);
        if (wf->winref != 0)
            continue;   /* already assigned */

        auto it = g_pending_wins.find(wf);
        if (it == g_pending_wins.end())
            continue;
        PendingWinSpec &spec = it->second;

        /* Build partition clause (SortGroupClause list).
         * Each partition expression needs a tlist entry with ressortgroupref. */
        List *partition_clause = NIL;
        ListCell *plc;
        foreach(plc, spec.partition_exprs)
        {
            Expr *pexpr = (Expr *)lfirst(plc);

            /* Find or add matching tlist entry. */
            TargetEntry *match = NULL;
            ListCell *tlc;
            foreach(tlc, query->targetList)
            {
                TargetEntry *tle = (TargetEntry *)lfirst(tlc);
                if (equal(tle->expr, pexpr))
                {
                    match = tle;
                    break;
                }
            }
            if (match == NULL)
            {
                match = makeTargetEntry(
                    (Expr *)copyObjectImpl(pexpr),
                    (AttrNumber)(list_length(query->targetList) + 1),
                    pstrdup("partition_col"),
                    true);   /* resjunk */
                query->targetList = lappend(query->targetList, match);
            }
            if (match->ressortgroupref == 0)
                match->ressortgroupref = next_sgref++;

            Oid coltype = exprType((Node *)pexpr);
            Oid ltOpr, eqOpr;
            bool isHashable;
            get_sort_group_operators(coltype, true, true, false,
                                     &ltOpr, &eqOpr, NULL, &isHashable);

            SortGroupClause *sgc = makeNode(SortGroupClause);
            sgc->tleSortGroupRef = match->ressortgroupref;
            sgc->eqop = eqOpr;
            sgc->sortop = ltOpr;
            sgc->nulls_first = false;
            sgc->hashable = isHashable;
            partition_clause = lappend(partition_clause, sgc);
        }

        /* Build order clause. */
        List *order_clause = NIL;
        if (spec.sort_exprs != NIL)
        {
            int si = 0;
            ListCell *slc, *dlc;
            forboth(slc, spec.sort_exprs, dlc, spec.sort_dirs)
            {
                Expr *sexpr = (Expr *)lfirst(slc);
                int dir = lfirst_int(dlc);

                TargetEntry *match = NULL;
                ListCell *tlc;
                foreach(tlc, query->targetList)
                {
                    TargetEntry *tle = (TargetEntry *)lfirst(tlc);
                    if (equal(tle->expr, sexpr))
                    {
                        match = tle;
                        break;
                    }
                }
                if (match == NULL)
                {
                    match = makeTargetEntry(
                        (Expr *)copyObjectImpl(sexpr),
                        (AttrNumber)(list_length(query->targetList) + 1),
                        pstrdup("sort_col"),
                        true);
                    query->targetList = lappend(query->targetList, match);
                }
                if (match->ressortgroupref == 0)
                    match->ressortgroupref = next_sgref++;

                bool is_desc =
                    (dir == substrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST ||
                     dir == substrait::SortField::SORT_DIRECTION_DESC_NULLS_LAST);
                bool nulls_first =
                    (dir == substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST ||
                     dir == substrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST);

                Oid coltype = exprType((Node *)sexpr);
                Oid ltOpr, eqOpr, gtOpr;
                bool isHashable;
                get_sort_group_operators(coltype, !is_desc, true, is_desc,
                                         &ltOpr, &eqOpr, &gtOpr, &isHashable);

                SortGroupClause *sgc = makeNode(SortGroupClause);
                sgc->tleSortGroupRef = match->ressortgroupref;
                sgc->eqop = eqOpr;
                sgc->sortop = is_desc ? gtOpr : ltOpr;
                sgc->reverse_sort = is_desc;
                sgc->nulls_first = nulls_first;
                sgc->hashable = isHashable;
                order_clause = lappend(order_clause, sgc);
                si++;
            }
        }

        /* Create WindowClause. */
        WindowClause *wc = makeNode(WindowClause);
        wc->name = NULL;
        wc->refname = NULL;
        wc->partitionClause = partition_clause;
        wc->orderClause = order_clause;
        wc->frameOptions = spec.frame_options;
        wc->startOffset = spec.start_offset;
        wc->endOffset = spec.end_offset;
        wc->startInRangeFunc = InvalidOid;
        wc->endInRangeFunc = InvalidOid;
        wc->inRangeColl = InvalidOid;
        wc->inRangeAsc = true;
        wc->inRangeNullsFirst = false;
        wc->winref = next_winref;
        wc->copiedOrder = false;

        query->windowClause = lappend(query->windowClause, wc);
        wf->winref = next_winref;

        /* Assign same winref to other WindowFuncs with identical spec. */
        ListCell *wlc2;
        foreach(wlc2, wfuncs)
        {
            WindowFunc *wf2 = (WindowFunc *)lfirst(wlc2);
            if (wf2 == wf || wf2->winref != 0)
                continue;
            auto it2 = g_pending_wins.find(wf2);
            if (it2 == g_pending_wins.end())
                continue;
            PendingWinSpec &s2 = it2->second;
            if (equal(s2.partition_exprs, spec.partition_exprs) &&
                equal(s2.sort_exprs, spec.sort_exprs) &&
                s2.frame_options == spec.frame_options &&
                equal(s2.start_offset, spec.start_offset) &&
                equal(s2.end_offset, spec.end_offset))
            {
                wf2->winref = next_winref;
            }
        }
        next_winref++;
    }

    /* Cleanup. */
    foreach(wlc, wfuncs)
    {
        WindowFunc *wf = (WindowFunc *)lfirst(wlc);
        g_pending_wins.erase(wf);
    }
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
        throw SubstraitError("substrait: could not find count(*) in pg_proc");

    return result;
}

static List *
aggregate_rel_to_targetlist(const substrait::AggregateRel &agg,
                            Query *query,
                            const SubstraitSchema &schema,
                            const FuncMap &func_map,
                            const VirtualMap &virtuals = {},
                            RelCache *rel_cache = nullptr, int depth = 0)
{
    query->hasAggs = true;

    List *tlist = NIL;
    AttrNumber resno = 1;

    /* --- Grouping keys --- */
    /* Collect unique grouping keys across all groupings first to avoid
     * duplicates when multiple groupings reference the same field
     * (e.g. ROLLUP(a,b) → groupings [a,b], [a], []). */
    std::unordered_map<int, Index> field_to_sgref;

    for (const auto &grouping : agg.groupings())
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
        for (const auto &gexpr : grouping.grouping_expressions())
        {
#pragma GCC diagnostic pop
            if (!gexpr.has_selection() ||
                !gexpr.selection().has_direct_reference() ||
                !gexpr.selection().direct_reference().has_struct_field())
                throw SubstraitError("substrait: non-field grouping key not supported");

            int field_idx =
                gexpr.selection().direct_reference().struct_field().field();

            if (field_to_sgref.count(field_idx))
                continue;  /* already emitted */

            if (field_idx < 0 || field_idx >= (int)schema.size())
                throw SubstraitError(sfmt("substrait: grouping field index %d out of range",
                                          field_idx));

            auto [rt, attnum, typeoid, nr] = schema[field_idx];

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
                if (field_rte->rtekind == RTE_RELATION)
                    attname = get_attname(field_rte->relid, attnum, false);
                else
                    attname = pstrdup(strVal(list_nth(field_rte->eref->colnames, attnum - 1)));
            }

            Index sortgroupref = (Index)resno;
            field_to_sgref[field_idx] = sortgroupref;

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

    /* PG 18 requires RTE_GROUP for queries with GROUP BY.
     * The parser always creates one; the planner's flatten_group_exprs()
     * replaces GROUP-RTE Vars back to originals before planning. */
    if (query->groupClause != NIL)
    {
        RangeTblEntry *grte = makeNode(RangeTblEntry);
        grte->rtekind = RTE_GROUP;
        grte->lateral = false;
        grte->inFromCl = false;

        List *groupexprs = NIL;
        List *colnames = NIL;
        ListCell *lc;
        foreach(lc, query->groupClause)
        {
            SortGroupClause *sgc = (SortGroupClause *)lfirst(lc);
            /* Find TLE matching this sgref. */
            TargetEntry *tle = NULL;
            ListCell *lc2;
            foreach(lc2, tlist)
            {
                TargetEntry *t = (TargetEntry *)lfirst(lc2);
                if (t->ressortgroupref == sgc->tleSortGroupRef)
                { tle = t; break; }
            }
            if (!tle)
                throw SubstraitError("substrait: groupClause ref not found in tlist");
            groupexprs = lappend(groupexprs, copyObjectImpl(tle->expr));
            colnames = lappend(colnames,
                makeString(pstrdup(tle->resname ? tle->resname : "?column?")));
        }
        grte->groupexprs = groupexprs;
        grte->eref = makeAlias(pstrdup("*GROUP*"), colnames);

        query->rtable = lappend(query->rtable, grte);
        int group_rtindex = list_length(query->rtable);
        query->hasGroupRTE = true;

        /* Rewrite grouping column TLEs to reference the GROUP RTE. */
        AttrNumber gattno = 1;
        foreach(lc, query->groupClause)
        {
            SortGroupClause *sgc = (SortGroupClause *)lfirst(lc);
            ListCell *lc2;
            foreach(lc2, tlist)
            {
                TargetEntry *tle = (TargetEntry *)lfirst(lc2);
                if (tle->ressortgroupref == sgc->tleSortGroupRef)
                {
                    Oid t = exprType((Node *)tle->expr);
                    int32 tm = exprTypmod((Node *)tle->expr);
                    Oid coll = exprCollation((Node *)tle->expr);
                    tle->expr = (Expr *)makeVar(group_rtindex, gattno, t, tm, coll, 0);
                    break;
                }
            }
            gattno++;
        }
    }

    /* If multiple groupings → GROUPING SETS (covers ROLLUP/CUBE patterns). */
    if (agg.groupings_size() > 1)
    {
        List *sets = NIL;
        for (const auto &grouping : agg.groupings())
        {
            List *cols = NIL;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
            for (const auto &gexpr : grouping.grouping_expressions())
            {
#pragma GCC diagnostic pop
                int field_idx =
                    gexpr.selection().direct_reference().struct_field().field();
                cols = lappend_int(cols, (int)field_to_sgref[field_idx]);
            }
            GroupingSet *gs = makeNode(GroupingSet);
            gs->kind = (cols == NIL) ? GROUPING_SET_EMPTY
                                     : GROUPING_SET_SIMPLE;
            gs->content = cols;
            gs->location = -1;
            sets = lappend(sets, gs);
        }
        GroupingSet *wrapper = makeNode(GroupingSet);
        wrapper->kind = GROUPING_SET_SETS;
        wrapper->content = sets;
        wrapper->location = -1;
        query->groupingSets = list_make1(wrapper);
    }

    /* --- Aggregate measures --- */
    for (const auto &measure : agg.measures())
    {
        const auto &amf = measure.measure();

        auto fit = func_map.find(amf.function_reference());
        if (fit == func_map.end())
            throw SubstraitError(sfmt("substrait: unknown aggregate function anchor %u",
                                      amf.function_reference()));

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
                    throw SubstraitError("substrait: enum/type agg args unsupported");
                Expr *e = convert_expression(
                    arg.value(), schema, func_map, virtuals,
                    nullptr, rel_cache, depth);
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
            throw SubstraitError(sfmt("substrait: pg_proc entry missing for OID %u",
                                      aggfnoid));
        Oid rettype = ((Form_pg_proc)GETSTRUCT(proctup))->prorettype;
        ReleaseSysCache(proctup);

        List *agg_args = NIL;
        List *aggargtypes = NIL;
        if (!is_star)
        {
            for (size_t i = 0; i < converted_args.size(); i++)
            {
                TargetEntry *arg_tle =
                    makeTargetEntry(converted_args[i],
                                    (AttrNumber)(i + 1),
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
        if (amf.invocation() ==
            substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT)
        {
            List *distincts = NIL;
            for (int i = 0; i < list_length(agg_args); i++)
            {
                TargetEntry *arg_tle = (TargetEntry *)list_nth(agg_args, i);
                Oid argtype = exprType((Node *)arg_tle->expr);
                Oid ltOpr, eqOpr;
                bool isHashable;
                get_sort_group_operators(argtype, true, true, false,
                                         &ltOpr, &eqOpr, NULL, &isHashable);
                SortGroupClause *sgc = makeNode(SortGroupClause);
                sgc->tleSortGroupRef = arg_tle->ressortgroupref = (Index)(i + 1);
                sgc->eqop = eqOpr;
                sgc->sortop = ltOpr;
                sgc->nulls_first = false;
                sgc->hashable = isHashable;
                distincts = lappend(distincts, sgc);
            }
            aggref->aggdistinct = distincts;
        }
        else
        {
            aggref->aggdistinct = NIL;
        }
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
    {"concat", "||"},
};

static Expr *
convert_expression(const substrait::Expression &expr,
                   const SubstraitSchema &schema,
                   const FuncMap &func_map,
                   const VirtualMap &virtuals,
                   const SubstraitSchema *outer_schema,
                   RelCache *rel_cache, int depth)
{
    /* --- FieldReference → Var (or virtual expression) --- */
    if (expr.has_selection())
    {
        const auto &sel = expr.selection();
        if (!sel.has_direct_reference() ||
            !sel.direct_reference().has_struct_field())
            throw SubstraitError("substrait: unsupported field reference type");

        int idx = sel.direct_reference().struct_field().field();

        /* Outer reference: column from enclosing query (correlated subquery). */
        if (sel.has_outer_reference())
        {
            if (!outer_schema || idx < 0 || idx >= (int)outer_schema->size())
                throw SubstraitError(sfmt("substrait: outer reference %d out of range",
                                          idx));
            auto [rt, attnum, typeoid, nr] = (*outer_schema)[idx];
            Oid collation = is_string_type(typeoid)
                ? DEFAULT_COLLATION_OID : InvalidOid;
            Var *v = makeVar(rt, attnum, typeoid, -1, collation, 0);
            v->varlevelsup = sel.outer_reference().steps_out();
            return (Expr *)v;
        }

        if (idx < 0 || idx >= (int)schema.size())
            throw SubstraitError(sfmt("substrait: field index %d out of range", idx));

        /* Virtual column from inner ProjectRel — return copy of stored expr. */
        auto vit = virtuals.find(idx);
        if (vit != virtuals.end())
            return (Expr *)copyObjectImpl(vit->second);

        auto [rt, attnum, typeoid, nullingrels] = schema[idx];
        Oid collation = is_string_type(typeoid)
            ? DEFAULT_COLLATION_OID : InvalidOid;
        Var *v = makeVar(rt, attnum, typeoid, -1, collation, 0);
        if (nullingrels)
            v->varnullingrels = bms_copy(nullingrels);
        return (Expr *)v;
    }

    /* --- Subquery → SubLink --- */
    if (expr.has_subquery())
    {
        const auto &sq = expr.subquery();
        if (sq.has_scalar())
        {
            /* Skip CTE dedup inside subqueries: the SubPlan must
             * read base tables so PG can use indexes (e.g. Q17). */
            Query *subq = build_query_for_rel(
                &sq.scalar().input(), func_map, nullptr, &schema,
                nullptr, depth + 1);

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
                &sp.tuples(), func_map, nullptr, &schema,
                nullptr, depth + 1);

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
                &inp.haystack(), func_map, nullptr, &schema,
                nullptr, depth + 1);

            /* Build testexpr: needle = Param(PARAM_SUBLINK) */
            Expr *needle = convert_expression(
                inp.needles(0), schema, func_map, virtuals, outer_schema, rel_cache, depth);
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

        throw SubstraitError("substrait: unsupported subquery type");
    }

    /* --- Literal → Const --- */
    if (expr.has_literal())
    {
        const auto &lit = expr.literal();

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
                                     false, true);
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
            iv->time = (int64)ids.seconds() * 1000000 + ids.microseconds();
            iv->day = ids.days();
            iv->month = 0;
            return (Expr *)makeConst(INTERVALOID, -1, InvalidOid,
                                     sizeof(Interval),
                                     PointerGetDatum(iv),
                                     false, false);
        }

        /* Substrait timestamp: microseconds since Unix epoch (1970-01-01).
         * PG Timestamp: microseconds since PG epoch (2000-01-01).
         * Offset: 10957 days * 86400 sec * 1e6 us = 946684800000000 */
        static constexpr int64 UNIX_TO_PG_EPOCH_US = 946684800000000LL;

        if (lit.has_timestamp())
        {
            /* Deprecated field: value is microseconds since Unix epoch */
            int64 pg_ts = lit.timestamp() - UNIX_TO_PG_EPOCH_US;
            return (Expr *)makeConst(TIMESTAMPOID, -1, InvalidOid, 8,
                                     Int64GetDatum(pg_ts), false, true);
        }
        if (lit.has_timestamp_tz())
        {
            /* Deprecated field: microseconds since Unix epoch, with TZ */
            int64 pg_ts = lit.timestamp_tz() - UNIX_TO_PG_EPOCH_US;
            return (Expr *)makeConst(TIMESTAMPTZOID, -1, InvalidOid, 8,
                                     Int64GetDatum(pg_ts), false, true);
        }
        if (lit.has_precision_timestamp())
        {
            const auto &pt = lit.precision_timestamp();
            int64 us;
            switch (pt.precision())
            {
                case 0: us = pt.value() * 1000000; break; /* seconds */
                case 3: us = pt.value() * 1000;    break; /* milliseconds */
                case 6: us = pt.value();            break; /* microseconds */
                case 9: us = pt.value() / 1000;    break; /* nanoseconds */
                default:
                    throw SubstraitError(sfmt("substrait: unsupported timestamp precision %d",
                                              pt.precision()));
            }
            int64 pg_ts = us - UNIX_TO_PG_EPOCH_US;
            return (Expr *)makeConst(TIMESTAMPOID, -1, InvalidOid, 8,
                                     Int64GetDatum(pg_ts), false, true);
        }
        if (lit.has_precision_timestamp_tz())
        {
            const auto &pt = lit.precision_timestamp_tz();
            int64 us;
            switch (pt.precision())
            {
                case 0: us = pt.value() * 1000000; break;
                case 3: us = pt.value() * 1000;    break;
                case 6: us = pt.value();            break;
                case 9: us = pt.value() / 1000;    break;
                default:
                    throw SubstraitError(sfmt("substrait: unsupported timestamp_tz precision %d",
                                              pt.precision()));
            }
            int64 pg_ts = us - UNIX_TO_PG_EPOCH_US;
            return (Expr *)makeConst(TIMESTAMPTZOID, -1, InvalidOid, 8,
                                     Int64GetDatum(pg_ts), false, true);
        }
        if (lit.has_time())
        {
            /* Substrait time: microseconds since midnight.
             * PG TimeADT: also microseconds since midnight. */
            return (Expr *)makeConst(TIMEOID, -1, InvalidOid, 8,
                                     Int64GetDatum(lit.time()), false, true);
        }

        /* Typed NULL literal — e.g. null { decimal { ... } } */
        if (lit.has_null())
        {
            const auto &t = lit.null();
            Oid nulltype = UNKNOWNOID;
            int32 typmod = -1;
            if (t.has_bool_())        nulltype = BOOLOID;
            else if (t.has_i16())     nulltype = INT2OID;
            else if (t.has_i32())     nulltype = INT4OID;
            else if (t.has_i64())     nulltype = INT8OID;
            else if (t.has_fp32())    nulltype = FLOAT4OID;
            else if (t.has_fp64())    nulltype = FLOAT8OID;
            else if (t.has_string())  nulltype = TEXTOID;
            else if (t.has_varchar()) nulltype = VARCHAROID;
            else if (t.has_fixed_char()) nulltype = BPCHAROID;
            else if (t.has_date())    nulltype = DATEOID;
            else if (t.has_timestamp() || t.has_precision_timestamp())
                                       nulltype = TIMESTAMPOID;
            else if (t.has_timestamp_tz() || t.has_precision_timestamp_tz())
                                       nulltype = TIMESTAMPTZOID;
            else if (t.has_time())     nulltype = TIMEOID;
            else if (t.has_decimal())
            {
                nulltype = NUMERICOID;
                int32 prec = t.decimal().precision();
                int32 sc = t.decimal().scale();
                typmod = ((prec << 16) | sc) + VARHDRSZ;
            }
            int16 len = get_typlen(nulltype);
            bool byval = get_typbyval(nulltype);
            return (Expr *)makeConst(nulltype, typmod, InvalidOid,
                                     len, (Datum)0, true, byval);
        }

        throw SubstraitError("substrait: unsupported literal type");
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
                                         func_map, virtuals, outer_schema, rel_cache, depth);
            w->result = convert_expression(clause.then(), schema,
                                           func_map, virtuals, outer_schema, rel_cache, depth);
            w->location = -1;
            c->args = lappend(c->args, w);
        }

        if (ift.has_else_())
            c->defresult = convert_expression(ift.else_(), schema,
                                              func_map, virtuals, outer_schema, rel_cache, depth);

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
            throw SubstraitError("substrait: cast without input");

        Expr *input = convert_expression(cast.input(), schema,
                                         func_map, virtuals, outer_schema, rel_cache, depth);
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
            else if (t.has_timestamp() || t.has_precision_timestamp())
                dst_type = TIMESTAMPOID;
            else if (t.has_timestamp_tz() || t.has_precision_timestamp_tz())
                dst_type = TIMESTAMPTZOID;
            else if (t.has_time())
                dst_type = TIMEOID;
            else if (t.has_interval_day())
                dst_type = INTERVALOID;
            else if (t.has_interval_year())
                dst_type = INTERVALOID;
        }

        if (!OidIsValid(dst_type))
            throw SubstraitError("substrait: unsupported cast target type");

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
                if (OidIsValid(typmod_func))
                {
                    FuncExpr *tfe = makeNode(FuncExpr);
                    tfe->funcid = typmod_func;
                    tfe->funcresulttype = dst_type;
                    tfe->funcretset = false;
                    tfe->funcvariadic = false;
                    tfe->funcformat = COERCE_IMPLICIT_CAST;
                    tfe->funccollid = InvalidOid;
                    tfe->inputcollid = InvalidOid;
                    tfe->args = list_make3(result,
                        makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
                                  Int32GetDatum(dst_typmod), false, true),
                        makeBoolConst(false, false));
                    tfe->location = -1;
                    result = (Expr *)tfe;
                }
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
            CoerceViaIO *cio = makeNode(CoerceViaIO);
            cio->arg = input;
            cio->resulttype = dst_type;
            cio->resultcollid = InvalidOid;
            cio->coerceformat = COERCE_EXPLICIT_CAST;
            cio->location = -1;
            return (Expr *)cio;
        }

        throw SubstraitError(sfmt("substrait: no coercion path from %u to %u",
                                  src_type, dst_type));
    }

    /* --- ScalarFunction → OpExpr or BoolExpr --- */
    if (expr.has_scalar_function())
    {
        const auto &sf = expr.scalar_function();
        auto fit = func_map.find(sf.function_reference());
        if (fit == func_map.end())
            throw SubstraitError(sfmt("substrait: unknown function anchor %u",
                                      sf.function_reference()));

        const std::string &fname = fit->second;

        /* Boolean connectives */
        if (fname == "and" || fname == "or")
        {
            BoolExprType btype = (fname == "and") ? AND_EXPR : OR_EXPR;
            List *args = NIL;
            for (const auto &arg : sf.arguments())
            {
                if (!arg.has_value())
                    throw SubstraitError("substrait: non-value bool arg");
                args = lappend(args,
                               convert_expression(arg.value(),
                                                  schema, func_map,
                                                  virtuals, outer_schema, rel_cache, depth));
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
                throw SubstraitError("substrait: bad NOT expression");
            List *args = list_make1(
                convert_expression(sf.arguments(0).value(),
                                   schema, func_map, virtuals, outer_schema, rel_cache, depth));
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
                throw SubstraitError("substrait: bad null-test expression");
            NullTest *nt = makeNode(NullTest);
            nt->arg = convert_expression(sf.arguments(0).value(),
                                         schema, func_map, virtuals, outer_schema, rel_cache, depth);
            nt->nulltesttype = (fname == "is_null") ? IS_NULL : IS_NOT_NULL;
            nt->argisrow = false;
            nt->location = -1;
            return (Expr *)nt;
        }

        /* like → PG ~~ operator (text ~~ text) */
        if (fname == "like")
        {
            if (sf.arguments_size() != 2)
                throw SubstraitError("substrait: like expects 2 args");
            Expr *left = coerce_to_text(convert_expression(
                sf.arguments(0).value(), schema, func_map, virtuals, outer_schema, rel_cache, depth));
            Expr *right = coerce_to_text(convert_expression(
                sf.arguments(1).value(), schema, func_map, virtuals, outer_schema, rel_cache, depth));

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
                throw SubstraitError("substrait: extract expects 2 args");

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
                                                schema, func_map, virtuals, outer_schema, rel_cache, depth);
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
                throw SubstraitError("substrait: substring expects 2-3 args");
            Expr *str_arg = coerce_to_text(convert_expression(
                sf.arguments(0).value(), schema, func_map,
                virtuals, outer_schema, rel_cache, depth));
            Expr *start_arg = convert_expression(
                sf.arguments(1).value(), schema, func_map,
                virtuals, outer_schema, rel_cache, depth);

            List *args = list_make2(str_arg, start_arg);
            int nargs = 2;
            Oid argtypes[3] = {TEXTOID, INT4OID, INT4OID};
            if (sf.arguments_size() >= 3)
            {
                Expr *len_arg = convert_expression(
                    sf.arguments(2).value(), schema, func_map,
                    virtuals, outer_schema, rel_cache, depth);
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

        /* round(numeric, int) → PG round(numeric, int4) */
        if (fname == "round")
        {
            if (sf.arguments_size() != 2)
                throw SubstraitError("substrait: round expects 2 args");
            Expr *val = convert_expression(sf.arguments(0).value(),
                                           schema, func_map, virtuals, outer_schema, rel_cache, depth);
            Expr *scale = convert_expression(sf.arguments(1).value(),
                                             schema, func_map, virtuals, outer_schema, rel_cache, depth);

            /* Coerce first arg to numeric if needed (e.g. int8 from SUM). */
            Oid valtype = exprType((Node *)val);
            if (valtype != NUMERICOID)
            {
                CoercionPathType cpath;
                Oid cfuncid;
                cpath = find_coercion_pathway(NUMERICOID, valtype,
                                              COERCION_IMPLICIT, &cfuncid);
                if (cpath == COERCION_PATH_FUNC && OidIsValid(cfuncid))
                {
                    FuncExpr *cfe = makeNode(FuncExpr);
                    cfe->funcid = cfuncid;
                    cfe->funcresulttype = NUMERICOID;
                    cfe->funcretset = false;
                    cfe->funcvariadic = false;
                    cfe->funcformat = COERCE_IMPLICIT_CAST;
                    cfe->funccollid = InvalidOid;
                    cfe->inputcollid = InvalidOid;
                    cfe->args = list_make1(val);
                    cfe->location = -1;
                    val = (Expr *)cfe;
                }
            }

            /* Coerce second arg to int4 if needed. */
            Oid scaletype = exprType((Node *)scale);
            if (scaletype != INT4OID)
            {
                CoercionPathType cpath;
                Oid cfuncid;
                cpath = find_coercion_pathway(INT4OID, scaletype,
                                              COERCION_IMPLICIT, &cfuncid);
                if (cpath == COERCION_PATH_FUNC && OidIsValid(cfuncid))
                {
                    FuncExpr *cfe = makeNode(FuncExpr);
                    cfe->funcid = cfuncid;
                    cfe->funcresulttype = INT4OID;
                    cfe->funcretset = false;
                    cfe->funcvariadic = false;
                    cfe->funcformat = COERCE_IMPLICIT_CAST;
                    cfe->funccollid = InvalidOid;
                    cfe->inputcollid = InvalidOid;
                    cfe->args = list_make1(scale);
                    cfe->location = -1;
                    scale = (Expr *)cfe;
                }
            }

            Oid argtypes[2] = {NUMERICOID, INT4OID};
            List *funcname = list_make1(makeString(pstrdup("round")));
            Oid funcoid = LookupFuncName(funcname, 2, argtypes, false);

            Oid restype = NUMERICOID;
            int32 restypmod = -1;
            if (sf.has_output_type() && sf.output_type().has_decimal())
            {
                int32 p = sf.output_type().decimal().precision();
                int32 s = sf.output_type().decimal().scale();
                restypmod = ((p << 16) | s) + VARHDRSZ;
            }

            FuncExpr *fe = makeNode(FuncExpr);
            fe->funcid = funcoid;
            fe->funcresulttype = restype;
            fe->funcretset = false;
            fe->funcvariadic = false;
            fe->funcformat = COERCE_EXPLICIT_CALL;
            fe->funccollid = InvalidOid;
            fe->inputcollid = InvalidOid;
            fe->args = list_make2(val, scale);
            fe->location = -1;
            return (Expr *)fe;
        }

        if (fname == "abs")
        {
            if (sf.arguments_size() != 1)
                throw SubstraitError("substrait: abs expects 1 arg");
            Expr *arg = convert_expression(sf.arguments(0).value(),
                                           schema, func_map, virtuals, outer_schema, rel_cache, depth);
            Oid argtype = exprType((Node *)arg);
            Oid argtypes[1] = {argtype};
            List *funcname = list_make1(makeString(pstrdup("abs")));
            Oid funcoid = LookupFuncName(funcname, 1, argtypes, false);

            FuncExpr *fe = makeNode(FuncExpr);
            fe->funcid = funcoid;
            fe->funcresulttype = argtype;
            fe->funcretset = false;
            fe->funcvariadic = false;
            fe->funcformat = COERCE_EXPLICIT_CALL;
            fe->funccollid = InvalidOid;
            fe->inputcollid = InvalidOid;
            fe->args = list_make1(arg);
            fe->location = -1;
            return (Expr *)fe;
        }

        if (fname == "char_length" || fname == "character_length")
        {
            if (sf.arguments_size() != 1)
                throw SubstraitError("substrait: char_length expects 1 arg");
            Expr *arg = coerce_to_text(convert_expression(
                sf.arguments(0).value(), schema, func_map, virtuals, outer_schema, rel_cache, depth));
            Oid argtypes[1] = {TEXTOID};
            List *funcname = list_make1(makeString(pstrdup("char_length")));
            Oid funcoid = LookupFuncName(funcname, 1, argtypes, false);
            FuncExpr *fe = makeNode(FuncExpr);
            fe->funcid = funcoid;
            fe->funcresulttype = INT4OID;
            fe->funcretset = false;
            fe->funcvariadic = false;
            fe->funcformat = COERCE_EXPLICIT_CALL;
            fe->funccollid = InvalidOid;
            fe->inputcollid = DEFAULT_COLLATION_OID;
            fe->args = list_make1(arg);
            fe->location = -1;
            return (Expr *)fe;
        }

        /* upper(text) / lower(text) */
        if (fname == "upper" || fname == "lower")
        {
            if (sf.arguments_size() != 1)
                throw SubstraitError(sfmt("substrait: %s expects 1 arg", fname.c_str()));
            Expr *arg = coerce_to_text(convert_expression(
                sf.arguments(0).value(), schema, func_map, virtuals, outer_schema, rel_cache, depth));
            Oid argtypes[1] = {TEXTOID};
            List *funcname = list_make1(makeString(pstrdup(fname.c_str())));
            Oid funcoid = LookupFuncName(funcname, 1, argtypes, false);
            FuncExpr *fe = makeNode(FuncExpr);
            fe->funcid = funcoid;
            fe->funcresulttype = TEXTOID;
            fe->funcretset = false;
            fe->funcvariadic = false;
            fe->funcformat = COERCE_EXPLICIT_CALL;
            fe->funccollid = DEFAULT_COLLATION_OID;
            fe->inputcollid = DEFAULT_COLLATION_OID;
            fe->args = list_make1(arg);
            fe->location = -1;
            return (Expr *)fe;
        }

        /* is_not_distinct_from(a, b) → NOT (a IS DISTINCT FROM b) */
        if (fname == "is_not_distinct_from")
        {
            if (sf.arguments_size() != 2)
                throw SubstraitError("substrait: is_not_distinct_from expects 2 args");

            Expr *left = convert_expression(sf.arguments(0).value(),
                                            schema, func_map, virtuals, outer_schema, rel_cache, depth);
            Expr *right = convert_expression(sf.arguments(1).value(),
                                             schema, func_map, virtuals, outer_schema, rel_cache, depth);

            Oid lefttype = exprType((Node *)left);
            Oid righttype = exprType((Node *)right);

            /* Coerce string types. */
            if (is_string_type(lefttype) && is_string_type(righttype) &&
                (lefttype != TEXTOID || righttype != TEXTOID))
            {
                left = coerce_to_text(left);
                right = coerce_to_text(right);
                lefttype = TEXTOID;
                righttype = TEXTOID;
            }

            /* Look up the = operator for DistinctExpr. */
            List *opname = list_make1(makeString(pstrdup("=")));
            Oid opoid = LookupOperName(NULL, opname, lefttype, righttype,
                                       false, -1);
            HeapTuple optup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opoid));
            Form_pg_operator opform = (Form_pg_operator)GETSTRUCT(optup);

            /* DistinctExpr is IS DISTINCT FROM. */
            DistinctExpr *de = makeNode(DistinctExpr);
            de->opno = opoid;
            de->opfuncid = opform->oprcode;
            de->opresulttype = BOOLOID;
            de->opretset = false;
            de->opcollid = InvalidOid;
            de->inputcollid = (is_string_type(lefttype) || is_string_type(righttype))
                ? DEFAULT_COLLATION_OID : InvalidOid;
            de->args = list_make2(left, right);
            de->location = -1;
            ReleaseSysCache(optup);

            /* Negate: NOT (a IS DISTINCT FROM b) = a IS NOT DISTINCT FROM b. */
            BoolExpr *notexpr = makeNode(BoolExpr);
            notexpr->boolop = NOT_EXPR;
            notexpr->args = list_make1(de);
            notexpr->location = -1;
            return (Expr *)notexpr;
        }

        /* Operator-based functions (comparison + arithmetic) */
        auto opit = kOpNameMap.find(fname);
        if (opit != kOpNameMap.end())
        {
            if (sf.arguments_size() != 2)
                throw SubstraitError(sfmt("substrait: operator %s expects 2 args, got %d",
                                          fname.c_str(), sf.arguments_size()));

            Expr *left = convert_expression(sf.arguments(0).value(),
                                            schema, func_map,
                                            virtuals, outer_schema, rel_cache, depth);
            Expr *right = convert_expression(sf.arguments(1).value(),
                                             schema, func_map,
                                             virtuals, outer_schema, rel_cache, depth);

            Oid lefttype = exprType((Node *)left);
            Oid righttype = exprType((Node *)right);

            /* Coerce text/varchar/bpchar to a common type for operator lookup. */
            if (is_string_type(lefttype) && is_string_type(righttype) &&
                (lefttype != TEXTOID || righttype != TEXTOID))
            {
                left = coerce_to_text(left);
                right = coerce_to_text(right);
                lefttype = TEXTOID;
                righttype = TEXTOID;
            }

            /* Coerce mismatched numeric/integer types for operator lookup.
             * Only handle known-safe promotions to avoid planner issues. */
            if (lefttype != righttype)
            {
                auto try_coerce = [](Expr *e, Oid src, Oid dst) -> Expr * {
                    if (src == dst) return e;
                    CoercionPathType path;
                    Oid funcid;
                    path = find_coercion_pathway(dst, src,
                                                 COERCION_IMPLICIT, &funcid);
                    if (path == COERCION_PATH_FUNC && OidIsValid(funcid))
                    {
                        FuncExpr *fe = makeNode(FuncExpr);
                        fe->funcid = funcid;
                        fe->funcresulttype = dst;
                        fe->funcretset = false;
                        fe->funcvariadic = false;
                        fe->funcformat = COERCE_IMPLICIT_CAST;
                        fe->funccollid = InvalidOid;
                        fe->inputcollid = InvalidOid;
                        fe->args = list_make1(e);
                        fe->location = -1;
                        return (Expr *)fe;
                    }
                    if (path == COERCION_PATH_RELABELTYPE)
                    {
                        RelabelType *r = makeNode(RelabelType);
                        r->arg = e;
                        r->resulttype = dst;
                        r->resulttypmod = -1;
                        r->resultcollid = InvalidOid;
                        r->relabelformat = COERCE_IMPLICIT_CAST;
                        r->location = -1;
                        return (Expr *)r;
                    }
                    return NULL;
                };

                /* Try coercing right to left's type, then left to right's. */
                Expr *rcoerced = try_coerce(right, righttype, lefttype);
                if (rcoerced)
                {
                    right = rcoerced;
                    righttype = lefttype;
                }
                else
                {
                    Expr *lcoerced = try_coerce(left, lefttype, righttype);
                    if (lcoerced)
                    {
                        left = lcoerced;
                        lefttype = righttype;
                    }
                }
            }

            /* Fallback: one side string, other not (e.g. integer = character).
             * Coerce both to text so we can use the text operator. */
            if (lefttype != righttype &&
                (is_string_type(lefttype) != is_string_type(righttype)))
            {
                auto to_text = [](Expr *e, Oid t) -> Expr * {
                    if (t == TEXTOID) return e;
                    if (is_string_type(t)) return coerce_to_text(e);
                    CoerceViaIO *cio = makeNode(CoerceViaIO);
                    cio->arg = e;
                    cio->resulttype = TEXTOID;
                    cio->resultcollid = DEFAULT_COLLATION_OID;
                    cio->coerceformat = COERCE_IMPLICIT_CAST;
                    cio->location = -1;
                    return (Expr *)cio;
                };
                left = to_text(left, lefttype);
                right = to_text(right, righttype);
                lefttype = TEXTOID;
                righttype = TEXTOID;
            }

            List *opname = list_make1(makeString(pstrdup(opit->second)));
            Oid opoid = LookupOperName(NULL, opname, lefttype, righttype,
                                       false, -1);

            HeapTuple optup = SearchSysCache1(OPEROID,
                                              ObjectIdGetDatum(opoid));
            if (!HeapTupleIsValid(optup))
                throw SubstraitError(sfmt("substrait: operator %s not found",
                                          opit->second));
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

        throw SubstraitError(sfmt("substrait: unsupported scalar function \"%s\"",
                                  fname.c_str()));
    }

    /* --- WindowFunction → WindowFunc node --- */
    if (expr.has_window_function())
    {
        const auto &wf = expr.window_function();

        auto fit = func_map.find(wf.function_reference());
        if (fit == func_map.end())
            throw SubstraitError(sfmt("substrait: unknown window function anchor %u",
                                      wf.function_reference()));

        const std::string &substrait_name = fit->second;

        /* Resolve PG function OID — check agg map first, then window map,
         * then try the raw name. */
        std::string pg_name = substrait_name;
        {
            auto nit = kAggNameMap.find(substrait_name);
            if (nit != kAggNameMap.end())
                pg_name = nit->second;
            else
            {
                auto wit = kWinFuncNameMap.find(substrait_name);
                if (wit != kWinFuncNameMap.end())
                    pg_name = wit->second;
            }
        }

        /* Convert arguments. */
        List *args = NIL;
        std::vector<Oid> argtypes;
        for (int i = 0; i < wf.arguments_size(); i++)
        {
            const auto &arg = wf.arguments(i);
            if (!arg.has_value())
                throw SubstraitError("substrait: non-value window function arg");
            Expr *e = convert_expression(arg.value(), schema, func_map,
                                         virtuals, outer_schema, rel_cache, depth);
            args = lappend(args, e);
            argtypes.push_back(exprType((Node *)e));
        }

        /* Lookup function OID. */
        bool is_star = (pg_name == "count" && argtypes.empty());
        Oid winfnoid;
        if (is_star)
            winfnoid = lookup_count_star();
        else if (pg_name == "count")
            winfnoid = lookup_count_star();
        else
        {
            List *funcname = list_make1(makeString(pstrdup(pg_name.c_str())));
            winfnoid = LookupFuncName(funcname, (int)argtypes.size(),
                                      argtypes.data(), false);
        }

        /* Get return type. */
        HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(winfnoid));
        if (!HeapTupleIsValid(proctup))
            throw SubstraitError(sfmt("substrait: pg_proc missing for OID %u",
                                      winfnoid));
        Oid rettype = ((Form_pg_proc)GETSTRUCT(proctup))->prorettype;
        ReleaseSysCache(proctup);

        /* Check if this is an aggregate function (winagg flag). */
        bool winagg = false;
        {
            HeapTuple aggtup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(winfnoid));
            if (HeapTupleIsValid(aggtup))
            {
                winagg = true;
                ReleaseSysCache(aggtup);
            }
        }

        /* Build WindowFunc node. winref will be set by fixup_window_clauses. */
        WindowFunc *wfn = makeNode(WindowFunc);
        wfn->winfnoid = winfnoid;
        wfn->wintype = rettype;
        wfn->wincollid = is_string_type(rettype)
            ? DEFAULT_COLLATION_OID : InvalidOid;
        wfn->inputcollid = InvalidOid;
        for (int i = 0; i < (int)argtypes.size(); i++)
        {
            if (is_string_type(argtypes[i]))
            {
                wfn->inputcollid = DEFAULT_COLLATION_OID;
                break;
            }
        }
        wfn->args = args;
        wfn->aggfilter = NULL;
        wfn->runCondition = NIL;
        wfn->winref = 0;   /* fixed up later */
        wfn->winstar = is_star;
        wfn->winagg = winagg;
        wfn->location = -1;

        /* Convert partitions and sorts for deferred WindowClause creation. */
        PendingWinSpec pws;
        pws.partition_exprs = NIL;
        for (int i = 0; i < wf.partitions_size(); i++)
        {
            Expr *pe = convert_expression(wf.partitions(i), schema,
                                          func_map, virtuals, outer_schema, rel_cache, depth);
            pws.partition_exprs = lappend(pws.partition_exprs, pe);
        }

        pws.sort_exprs = NIL;
        pws.sort_dirs = NIL;
        for (int i = 0; i < wf.sorts_size(); i++)
        {
            const auto &sf = wf.sorts(i);
            if (sf.has_expr())
            {
                Expr *se = convert_expression(sf.expr(), schema,
                                              func_map, virtuals, outer_schema, rel_cache, depth);
                pws.sort_exprs = lappend(pws.sort_exprs, se);
                int dir = sf.has_direction()
                    ? (int)sf.direction()
                    : (int)substrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST;
                pws.sort_dirs = lappend_int(pws.sort_dirs, dir);
            }
        }

        pws.frame_options = substrait_bounds_to_frame(wf,
            &pws.start_offset, &pws.end_offset);

        g_pending_wins[wfn] = pws;
        return (Expr *)wfn;
    }

    throw SubstraitError("substrait: unsupported expression type");
}

/* ============================================================
 * SECTION 6 - Adapter-level decorrelation of scalar subqueries
 *
 * WHY ADAPTER, NOT ISTHMUS: Calcite's decorrelation is a black box that can
 * produce bad plans (cross joins, wrong join ordering). The adapter produces PG
 * query trees directly, letting PG's planner choose the optimal strategy.
 *
 * WHY NOT THE hasScalarCorrelate GATE: Both TPC-H Q2 (min(...)) and TPC-DS Q01
 * (avg(...)*1.2) are correlated scalar subqueries with aggregates. No
 * isthmus-level heuristic can distinguish "decorrelation helps" from
 * "decorrelation hurts" — only PG's cost-based optimizer can.
 *
 * WHAT THIS DOES: Rewrites EXPR_SUBLINK (correlated scalar subquery, per-row
 * execution) → LEFT JOIN (decorrelated, PG picks hash/merge/NL). Only handles
 * simple equality correlations; complex patterns stay as EXPR_SUBLINK.
 *
 * SINGLE_VALUE GATE IN ISTHMUS: Still handles subqueries without aggregates
 * (Calcite wraps in SINGLE_VALUE). The adapter handles the ones isthmus misses
 * (subqueries with aggregates that guarantee single-row).
 * ============================================================ */

/* Return true if any Var in the expression tree has varlevelsup > 0. */
static bool
has_outer_refs_walker(Node *node, void *context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var))
        return ((Var *)node)->varlevelsup > 0;
    if (IsA(node, Query))
        return false;   /* don't descend into sub-Queries */
    return expression_tree_walker(node,
        (bool (*)()) has_outer_refs_walker, context);
}

static bool
has_outer_refs(Node *node)
{
    return has_outer_refs_walker(node, NULL);
}

/*
 * Decompose a WHERE clause into correlated equalities (inner_var = outer_var)
 * and local quals. Only handles simple OpExpr equalities where exactly one
 * side has outer refs. Non-equality correlations are left in local_quals.
 */
static void
split_correlation_quals(Node *quals, List **corr_quals, Node **local_quals)
{
    *corr_quals = NIL;
    *local_quals = NULL;

    if (quals == NULL)
        return;

    List *qual_list = NIL;
    if (IsA(quals, BoolExpr) && ((BoolExpr *)quals)->boolop == AND_EXPR)
        qual_list = ((BoolExpr *)quals)->args;
    else
        qual_list = list_make1(quals);

    List *local_list = NIL;
    ListCell *lc;
    foreach(lc, qual_list)
    {
        Node *qual = (Node *)lfirst(lc);
        if (IsA(qual, OpExpr))
        {
            OpExpr *op = (OpExpr *)qual;
            if (list_length(op->args) == 2)
            {
                Node *left = (Node *)linitial(op->args);
                Node *right = (Node *)lsecond(op->args);
                bool left_outer = has_outer_refs(left);
                bool right_outer = has_outer_refs(right);

                if (left_outer != right_outer)
                {
                    /* Check if this is an equality operator. */
                    HeapTuple optup = SearchSysCache1(OPEROID,
                        ObjectIdGetDatum(op->opno));
                    if (HeapTupleIsValid(optup))
                    {
                        Form_pg_operator opform =
                            (Form_pg_operator)GETSTRUCT(optup);
                        bool is_eq =
                            (strcmp(NameStr(opform->oprname), "=") == 0);
                        ReleaseSysCache(optup);
                        if (is_eq)
                        {
                            *corr_quals = lappend(*corr_quals, qual);
                            continue;
                        }
                    }
                    /* else: invalid tuple, nothing to release */
                }
            }
        }
        local_list = lappend(local_list, qual);
    }

    if (local_list == NIL)
        *local_quals = NULL;
    else if (list_length(local_list) == 1)
        *local_quals = (Node *)linitial(local_list);
    else
    {
        BoolExpr *and_expr = makeNode(BoolExpr);
        and_expr->boolop = AND_EXPR;
        and_expr->args = local_list;
        and_expr->location = -1;
        *local_quals = (Node *)and_expr;
    }
}

struct PullupContext {
    Query *query;
};

static Node *
pullup_expr_sublinks_mutator(Node *node, void *context)
{
    if (node == NULL)
        return NULL;

    if (IsA(node, SubLink))
    {
        SubLink *sl = (SubLink *)node;
        if (sl->subLinkType != EXPR_SUBLINK)
            return node;

        Query *subq = (Query *)sl->subselect;
        if (!has_outer_refs(subq->jointree->quals))
            return node;

        List *corr_quals;
        Node *local_quals;
        split_correlation_quals(subq->jointree->quals,
                                &corr_quals, &local_quals);
        if (corr_quals == NIL)
            return node;    /* no simple equality correlations — bail */

        PullupContext *ctx = (PullupContext *)context;
        Query *outer = ctx->query;

        /* --- Add GROUP BY columns to subquery --- */
        Index next_sgref = 1;
        {
            ListCell *lc;
            foreach(lc, subq->targetList)
            {
                TargetEntry *tle = (TargetEntry *)lfirst(lc);
                if (tle->ressortgroupref >= next_sgref)
                    next_sgref = tle->ressortgroupref + 1;
            }
        }

        int first_grp_attno = list_length(subq->targetList) + 1;
        {
            ListCell *lc;
            foreach(lc, corr_quals)
            {
                OpExpr *op = (OpExpr *)lfirst(lc);
                Node *left = (Node *)linitial(op->args);
                Node *right = (Node *)lsecond(op->args);

                /* inner_var = the side without outer refs */
                Expr *inner_var = has_outer_refs(left)
                    ? (Expr *)right : (Expr *)left;

                TargetEntry *grp_tle = makeTargetEntry(
                    (Expr *)copyObjectImpl(inner_var),
                    (AttrNumber)(list_length(subq->targetList) + 1),
                    pstrdup("grp_key"),
                    false);
                grp_tle->ressortgroupref = next_sgref;
                subq->targetList = lappend(subq->targetList, grp_tle);

                Oid typeoid = exprType((Node *)inner_var);
                Oid ltop, eqop;
                bool hashable;
                get_sort_group_operators(typeoid, true, true, false,
                                         &ltop, &eqop, NULL, &hashable);

                SortGroupClause *sgc = makeNode(SortGroupClause);
                sgc->tleSortGroupRef = next_sgref++;
                sgc->eqop = eqop;
                sgc->sortop = ltop;
                sgc->nulls_first = false;
                sgc->hashable = hashable;
                subq->groupClause = lappend(subq->groupClause, sgc);
            }
        }

        /* Rebuild subquery WHERE: only local quals remain. */
        subq->jointree->quals = local_quals;

        /* --- Create RTE_SUBQUERY for the modified subquery --- */
        RangeTblEntry *sub_rte = makeNode(RangeTblEntry);
        sub_rte->rtekind = RTE_SUBQUERY;
        sub_rte->subquery = subq;
        sub_rte->lateral = false;
        sub_rte->inh = false;
        sub_rte->inFromCl = true;

        List *colnames = NIL;
        {
            ListCell *lc;
            foreach(lc, subq->targetList)
            {
                TargetEntry *tle = (TargetEntry *)lfirst(lc);
                colnames = lappend(colnames,
                    makeString(pstrdup(tle->resname
                        ? tle->resname : "?column?")));
            }
        }
        sub_rte->eref = makeAlias(pstrdup("decorr"), colnames);

        outer->rtable = lappend(outer->rtable, sub_rte);
        int sub_rtindex = list_length(outer->rtable);

        /* --- Build LEFT JOIN condition --- */
        List *join_qual_list = NIL;
        int grp_attno = first_grp_attno;
        {
            ListCell *lc;
            foreach(lc, corr_quals)
            {
                OpExpr *op = (OpExpr *)lfirst(lc);
                Node *left = (Node *)linitial(op->args);
                Node *right = (Node *)lsecond(op->args);

                bool left_is_outer = has_outer_refs(left);
                Var *outer_var = (Var *)copyObjectImpl(
                    left_is_outer ? left : right);
                Expr *inner_var = left_is_outer
                    ? (Expr *)right : (Expr *)left;

                /* Demote outer_var: varlevelsup 1→0 (now same-level). */
                outer_var->varlevelsup = 0;

                /* Reference the GROUP BY column in the new RTE. */
                Oid inner_type = exprType((Node *)inner_var);
                Var *sub_var = makeVar(sub_rtindex,
                    (AttrNumber)grp_attno++, inner_type, -1,
                    is_string_type(inner_type)
                        ? DEFAULT_COLLATION_OID : InvalidOid, 0);

                OpExpr *eq = makeNode(OpExpr);
                eq->opno = op->opno;
                eq->opfuncid = op->opfuncid;
                eq->opresulttype = BOOLOID;
                eq->opretset = false;
                eq->opcollid = InvalidOid;
                eq->inputcollid = op->inputcollid;
                eq->args = list_make2(outer_var, sub_var);
                eq->location = -1;

                join_qual_list = lappend(join_qual_list, eq);
            }
        }

        Node *join_quals;
        if (list_length(join_qual_list) == 1)
            join_quals = (Node *)linitial(join_qual_list);
        else
        {
            BoolExpr *and_expr = makeNode(BoolExpr);
            and_expr->boolop = AND_EXPR;
            and_expr->args = join_qual_list;
            and_expr->location = -1;
            join_quals = (Node *)and_expr;
        }

        /* --- RTE_JOIN for the planner --- */
        List *alias_vars = NIL;
        List *join_colnames = NIL;

        /* Left side: all existing RTEs' columns. */
        for (int i = 1; i < sub_rtindex; i++)
        {
            RangeTblEntry *rte =
                (RangeTblEntry *)list_nth(outer->rtable, i - 1);
            if (rte->rtekind == RTE_RELATION)
            {
                Relation rel = RelationIdGetRelation(rte->relid);
                TupleDesc td = RelationGetDescr(rel);
                for (int j = 0; j < td->natts; j++)
                {
                    Form_pg_attribute attr = TupleDescAttr(td, j);
                    if (attr->attisdropped)
                        continue;
                    alias_vars = lappend(alias_vars,
                        makeVar(i, attr->attnum, attr->atttypid,
                                attr->atttypmod, attr->attcollation, 0));
                    join_colnames = lappend(join_colnames,
                        makeString(pstrdup(NameStr(attr->attname))));
                }
                RelationClose(rel);
            }
            else if (rte->rtekind == RTE_SUBQUERY)
            {
                AttrNumber att = 1;
                ListCell *lc2;
                foreach(lc2, rte->subquery->targetList)
                {
                    TargetEntry *tle = (TargetEntry *)lfirst(lc2);
                    Oid t = exprType((Node *)tle->expr);
                    alias_vars = lappend(alias_vars,
                        makeVar(i, att, t, -1,
                                is_string_type(t)
                                    ? DEFAULT_COLLATION_OID : InvalidOid, 0));
                    join_colnames = lappend(join_colnames,
                        makeString(pstrdup(tle->resname
                            ? tle->resname : "?column?")));
                    att++;
                }
            }
            else if (rte->rtekind == RTE_JOIN)
            {
                /* Skip — join RTEs don't contribute columns directly. */
            }
        }

        /* Right side: subquery columns (nullable in LEFT JOIN). */
        int decorr_join_rtindex = list_length(outer->rtable) + 1;
        {
            AttrNumber att = 1;
            ListCell *lc2;
            foreach(lc2, subq->targetList)
            {
                TargetEntry *tle = (TargetEntry *)lfirst(lc2);
                Oid t = exprType((Node *)tle->expr);
                Var *v = makeVar(sub_rtindex, att, t, -1,
                            is_string_type(t)
                                ? DEFAULT_COLLATION_OID : InvalidOid, 0);
                v->varnullingrels = bms_make_singleton(decorr_join_rtindex);
                alias_vars = lappend(alias_vars, v);
                join_colnames = lappend(join_colnames,
                    makeString(pstrdup(tle->resname
                        ? tle->resname : "?column?")));
                att++;
            }
        }

        /* Also set varnullingrels on sub_var refs in join quals. */
        {
            ListCell *lc2;
            foreach(lc2, join_qual_list)
            {
                OpExpr *eq = (OpExpr *)lfirst(lc2);
                ListCell *alc;
                foreach(alc, eq->args)
                {
                    Node *n = (Node *)lfirst(alc);
                    if (IsA(n, Var))
                    {
                        Var *v = (Var *)n;
                        if (v->varno == sub_rtindex)
                            v->varnullingrels = bms_make_singleton(decorr_join_rtindex);
                    }
                }
            }
        }

        RangeTblEntry *join_rte = makeNode(RangeTblEntry);
        join_rte->rtekind = RTE_JOIN;
        join_rte->jointype = JOIN_LEFT;
        join_rte->joinmergedcols = 0;
        join_rte->joinaliasvars = alias_vars;
        join_rte->joinleftcols = NIL;
        join_rte->joinrightcols = NIL;
        join_rte->join_using_alias = NULL;
        join_rte->eref = makeAlias(pstrdup("decorr_join"), join_colnames);
        join_rte->lateral = false;
        join_rte->inh = false;
        join_rte->inFromCl = true;

        outer->rtable = lappend(outer->rtable, join_rte);
        int join_rtindex = list_length(outer->rtable);

        /* --- Create LEFT JoinExpr --- */
        RangeTblRef *sub_rtr = makeNode(RangeTblRef);
        sub_rtr->rtindex = sub_rtindex;

        /* Wrap existing fromlist as larg of the LEFT JOIN. */
        Node *left_arg;
        if (list_length(outer->jointree->fromlist) == 1)
        {
            left_arg = (Node *)linitial(outer->jointree->fromlist);
        }
        else
        {
            /* Multiple items → chain into INNER JoinExprs. */
            left_arg = (Node *)linitial(outer->jointree->fromlist);
            {
                ListCell *lc3;
                for (lc3 = lnext(outer->jointree->fromlist,
                                 list_head(outer->jointree->fromlist));
                     lc3 != NULL;
                     lc3 = lnext(outer->jointree->fromlist, lc3))
                {
                    JoinExpr *ij = makeNode(JoinExpr);
                    ij->jointype = JOIN_INNER;
                    ij->isNatural = false;
                    ij->larg = left_arg;
                    ij->rarg = (Node *)lfirst(lc3);
                    ij->quals = NULL;
                    ij->rtindex = 0;
                    left_arg = (Node *)ij;
                }
            }
        }

        JoinExpr *left_join = makeNode(JoinExpr);
        left_join->jointype = JOIN_LEFT;
        left_join->isNatural = false;
        left_join->larg = left_arg;
        left_join->rarg = (Node *)sub_rtr;
        left_join->usingClause = NIL;
        left_join->join_using_alias = NULL;
        left_join->quals = join_quals;
        left_join->alias = NULL;
        left_join->rtindex = join_rtindex;

        outer->jointree->fromlist = list_make1(left_join);

        /* Return Var referencing the aggregate output (attnum=1). */
        TargetEntry *agg_tle =
            (TargetEntry *)linitial(subq->targetList);
        Oid result_type = exprType((Node *)agg_tle->expr);
        Var *result_var = makeVar(sub_rtindex, 1, result_type, -1,
            is_string_type(result_type)
                ? DEFAULT_COLLATION_OID : InvalidOid, 0);
        result_var->varnullingrels = bms_make_singleton(join_rtindex);

        return (Node *)result_var;
    }

    /* Don't recurse into sub-Queries (they have their own scope). */
    if (IsA(node, Query))
        return node;

    return expression_tree_mutator(node,
        (Node *(*)()) pullup_expr_sublinks_mutator, context);
}

/*
 * Walk quals and targetList for EXPR_SUBLINK nodes with outer refs.
 * Rewrite each as a LEFT JOIN with GROUP BY on correlation keys.
 */
static void
pullup_correlated_scalar_sublinks(Query *query)
{
    PullupContext ctx;
    ctx.query = query;

    if (query->jointree->quals)
        query->jointree->quals = pullup_expr_sublinks_mutator(
            query->jointree->quals, &ctx);

    ListCell *lc;
    foreach(lc, query->targetList)
    {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        tle->expr = (Expr *)pullup_expr_sublinks_mutator(
            (Node *)tle->expr, &ctx);
    }
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
                    const SubstraitSchema *outer_schema,
                    RelCache *rel_cache, int depth)
{
    static constexpr int kMaxQueryDepth = 100;
    if (depth > kMaxQueryDepth)
        throw SubstraitError(
            sfmt("substrait: query nesting too deep (>%d levels)", kMaxQueryDepth));

    /* CTE dedup: check if this subtree was already built (hash-first). */
    std::string fp;
    bool is_dup = false;
    if (rel_cache)
    {
        FpHash h = fingerprint_rel_hash(*cur, rel_cache->hash_cache);
        if (rel_cache->dup_hashes.count(h))
        {
            const std::string &fp_ref = fingerprint_rel(*cur, *rel_cache);
            fp = fp_ref;
            if (rel_cache->duplicates.count(fp))
            {
                auto it = rel_cache->built.find(fp);
                if (it != rel_cache->built.end())
                {
                    /* Already built — return CTE reference. */
                    it->second.cte->cterefcount++;
                    return build_cte_ref_query(it->second, depth);
                }
                is_dup = true;
            }
        }
    }

    /*
     * If this is the first occurrence of a duplicate subtree, we'll wrap
     * it as a CTE.  Suppress nested CTE dedup inside the body: PG plans
     * CTE bodies in their own planner context, so inner CTEs at a deeper
     * depth would produce "bad levelsup" errors.
     */
    RelCache *outer_cache = nullptr;
    if (is_dup)
    {
        outer_cache = rel_cache;
        rel_cache = nullptr;
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
    bool is_set = cur->has_set();

    /* If the inner node (after peeling wrappers) is a known duplicate,
     * force the subquery path so the recursive call triggers CTE dedup.
     * Uses hash-first lookup to avoid unnecessary serialization. */
    if (rel_cache && (is_base || is_set))
    {
        FpHash h = fingerprint_rel_hash(*cur, rel_cache->hash_cache);
        if (rel_cache->dup_hashes.count(h))
        {
            const std::string &inner_fp = fingerprint_rel(*cur, *rel_cache);
            if (rel_cache->duplicates.count(inner_fp))
            {
                is_base = false;
                is_set = false;
            }
        }
    }

    /* SET under AGG must be wrapped as subquery — PG forbids
     * setOperations + groupClause on the same Query. */
    if (is_set && agg_rel != nullptr)
        is_set = false;

    /* SET under PROJECT with computed expressions must be wrapped —
     * window funcs / emit can create resjunk TLEs that PG's
     * postprocess_setop_tlist cannot handle on setop queries.
     * Pure passthrough (field refs only) is safe — don't wrap. */
    if (is_set && project_rel != nullptr &&
        project_rel->expressions_size() > 0)
    {
        bool has_computed = false;
        for (int i = 0; i < project_rel->expressions_size(); i++)
        {
            if (!project_rel->expressions(i).has_selection())
            {
                has_computed = true;
                break;
            }
        }
        if (has_computed)
            is_set = false;
    }

    if (is_set)
    {
        /* UNION ALL / UNION DISTINCT — build as PG SetOperationStmt. */
        const auto &setrel = cur->set();
        if (setrel.inputs_size() < 2)
            throw SubstraitError("substrait: set rel needs >= 2 inputs");

        SetOperation pg_op;
        bool set_all;
        switch (setrel.op())
        {
            case substrait::SetRel::SET_OP_UNION_ALL:
                pg_op = SETOP_UNION; set_all = true; break;
            case substrait::SetRel::SET_OP_UNION_DISTINCT:
                pg_op = SETOP_UNION; set_all = false; break;
            case substrait::SetRel::SET_OP_MINUS_PRIMARY:
                pg_op = SETOP_EXCEPT; set_all = false; break;
            case substrait::SetRel::SET_OP_MINUS_PRIMARY_ALL:
                pg_op = SETOP_EXCEPT; set_all = true; break;
            case substrait::SetRel::SET_OP_INTERSECTION_PRIMARY:
            case substrait::SetRel::SET_OP_INTERSECTION_MULTISET:
                pg_op = SETOP_INTERSECT; set_all = false; break;
            case substrait::SetRel::SET_OP_INTERSECTION_MULTISET_ALL:
                pg_op = SETOP_INTERSECT; set_all = true; break;
            default:
                throw SubstraitError(sfmt("substrait: unsupported set op %d",
                                          setrel.op()));
        }

        /* Build each input as a subquery RTE. */
        std::vector<int> rtindices;
        for (int i = 0; i < setrel.inputs_size(); i++)
        {
            Query *branch = build_query_for_rel(
                &setrel.inputs(i), func_map, nullptr, outer_schema,
                rel_cache, depth + 1);

            RangeTblEntry *rte = makeNode(RangeTblEntry);
            rte->rtekind = RTE_SUBQUERY;
            rte->subquery = branch;
            rte->lateral = false;
            rte->inh = false;
            rte->inFromCl = true;

            List *cnames = NIL;
            ListCell *lc2;
            foreach(lc2, branch->targetList)
            {
                TargetEntry *tle = (TargetEntry *)lfirst(lc2);
                cnames = lappend(cnames,
                    makeString(pstrdup(tle->resname ? tle->resname : "?column?")));
            }
            rte->eref = makeAlias(pstrdup(sfmt("setbranch%d", i).c_str()), cnames);

            query->rtable = lappend(query->rtable, rte);
            rtindices.push_back(list_length(query->rtable));
        }

        /* Build SetOperationStmt tree (left-associative for >2 inputs). */
        auto make_leaf = [](int rtindex) -> Node * {
            RangeTblRef *rtr = makeNode(RangeTblRef);
            rtr->rtindex = rtindex;
            return (Node *)rtr;
        };

        /* Collect colTypes/colTypmods/colCollations from first branch. */
        Query *first = ((RangeTblEntry *)list_nth(
            query->rtable, rtindices[0] - 1))->subquery;
        List *colTypes = NIL, *colTypmods = NIL, *colCollations = NIL;
        {
            ListCell *lc2;
            foreach(lc2, first->targetList)
            {
                TargetEntry *tle = (TargetEntry *)lfirst(lc2);
                Oid t = exprType((Node *)tle->expr);
                int32 tm = exprTypmod((Node *)tle->expr);
                Oid coll = is_string_type(t)
                    ? DEFAULT_COLLATION_OID : InvalidOid;
                colTypes = lappend_oid(colTypes, t);
                colTypmods = lappend_int(colTypmods, tm);
                colCollations = lappend_oid(colCollations, coll);
            }
        }

        /* For non-ALL set ops, PG needs groupClauses for dedup/sort. */
        List *groupClauses = NIL;
        if (!set_all)
        {
            ListCell *lc2;
            Index sgref = 1;
            foreach(lc2, first->targetList)
            {
                TargetEntry *tle = (TargetEntry *)lfirst(lc2);
                Oid coltype = exprType((Node *)tle->expr);
                Oid ltOpr, eqOpr;
                bool isHashable;
                get_sort_group_operators(coltype, true, true, false,
                                         &ltOpr, &eqOpr, NULL, &isHashable);
                SortGroupClause *sgc = makeNode(SortGroupClause);
                sgc->tleSortGroupRef = sgref++;
                sgc->eqop = eqOpr;
                sgc->sortop = ltOpr;
                sgc->nulls_first = false;
                sgc->hashable = isHashable;
                groupClauses = lappend(groupClauses, sgc);
            }
        }

        Node *setop = make_leaf(rtindices[0]);
        for (size_t i = 1; i < rtindices.size(); i++)
        {
            SetOperationStmt *s = makeNode(SetOperationStmt);
            s->op = pg_op;
            s->all = set_all;
            s->larg = setop;
            s->rarg = make_leaf(rtindices[i]);
            s->colTypes = colTypes;
            s->colTypmods = colTypmods;
            s->colCollations = colCollations;
            s->groupClauses = groupClauses;
            setop = (Node *)s;
        }
        query->setOperations = setop;

        /* Build target list from first branch's types. */
        AttrNumber resno = 1;
        {
            ListCell *lc2;
            foreach(lc2, first->targetList)
            {
                TargetEntry *tle = (TargetEntry *)lfirst(lc2);
                Oid t = exprType((Node *)tle->expr);
                int32 tm = exprTypmod((Node *)tle->expr);
                Oid coll = is_string_type(t)
                    ? DEFAULT_COLLATION_OID : InvalidOid;
                Var *v = makeVar(rtindices[0], resno, t, tm, coll, 0);
                TargetEntry *new_tle = makeTargetEntry(
                    (Expr *)v, resno,
                    pstrdup(tle->resname ? tle->resname : "?column?"),
                    false);
                if (!set_all)
                    new_tle->ressortgroupref = resno;
                query->targetList = lappend(query->targetList, new_tle);
                projected_schema.push_back({rtindices[0], resno, t, nullptr});
                resno++;
            }
        }
        schema = projected_schema;

        /* Apply filter (if any, e.g. HAVING on aggregate wrapping this set). */
        if (filter_rel != nullptr)
        {
            query->jointree->quals = (Node *)convert_expression(
                filter_rel->condition(), projected_schema, func_map,
                {}, outer_schema, rel_cache, depth);
        }
    }
    else if (is_base)
    {
        process_base_rel(*cur, query, schema, func_map, rel_cache, depth);

        /* Apply filter */
        if (filter_rel != nullptr)
        {
            Node *fexpr = (Node *)convert_expression(
                filter_rel->condition(), schema, func_map, {}, outer_schema, rel_cache, depth);

            /*
             * Crosses are flattened into fromlist items, so always use
             * WHERE. PG with join_collapse_limit=20 optimizes
             * FROM A, B, C WHERE ... into proper hash/merge joins.
             */
            if (query->jointree->quals == NULL)
                query->jointree->quals = fexpr;
            else
            {
                BoolExpr *and_expr = makeNode(BoolExpr);
                and_expr->boolop = AND_EXPR;
                and_expr->args = list_make2(query->jointree->quals, fexpr);
                and_expr->location = -1;
                query->jointree->quals = (Node *)and_expr;
            }
        }
        else if (cur->has_read() && cur->read().has_filter())
        {
            query->jointree->quals = (Node *)convert_expression(
                cur->read().filter(), schema, func_map, {}, outer_schema, rel_cache, depth);
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
                    if (base_idx < 0 || base_idx >= (int)schema.size())
                        throw SubstraitError(sfmt(
                            "substrait: projection index %d out of range (schema size %d)",
                            base_idx, (int)schema.size()));
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
                    projected_schema, func_map,
                    {}, nullptr, rel_cache, depth));
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
                        int vidx = src - n_input;
                        if (vidx < 0 || vidx >= (int)raw_virtuals.size())
                            throw SubstraitError(sfmt(
                                "substrait: emit mapping index %d out of range "
                                "(input=%d, virtuals=%d)",
                                src, n_input, (int)raw_virtuals.size()));
                        Expr *ve = raw_virtuals[vidx];
                        new_schema.push_back({0, 0, exprType((Node *)ve), nullptr});
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
                    projected_schema.push_back({0, 0, exprType((Node *)ve), nullptr});
                    virtual_map[n_input + (int)i] = ve;
                }
            }
        }
    }
    else
    {
        /* Nested rel chain (e.g., another Aggregate) — build as subquery. */
        Query *subq = build_query_for_rel(cur, func_map, nullptr, outer_schema,
                                          rel_cache, depth + 1);

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
            if (tle->resjunk)
                continue;
            colnames = lappend(colnames,
                makeString(pstrdup(tle->resname ? tle->resname : "?column?")));
            Oid t = exprType((Node *)tle->expr);
            projected_schema.push_back({0, (AttrNumber)attno, t, nullptr});
            attno++;
        }
        sub_rte->eref = makeAlias(pstrdup("subq"), colnames);

        query->rtable = lappend(query->rtable, sub_rte);
        int sub_rtindex = list_length(query->rtable);

        for (auto &[rt, attnum, typeoid, nullingrels] : projected_schema)
            rt = sub_rtindex;
        schema = projected_schema;

        RangeTblRef *rtr = makeNode(RangeTblRef);
        rtr->rtindex = sub_rtindex;
        query->jointree->fromlist = lappend(query->jointree->fromlist, rtr);

        /* Apply filter BEFORE inner_project reduces the schema. */
        if (filter_rel != nullptr && query->jointree->quals == NULL)
        {
            query->jointree->quals = (Node *)convert_expression(
                filter_rel->condition(), projected_schema, func_map,
                virtual_map, outer_schema, rel_cache, depth);
        }

        /* Apply inner_project on subquery output if present. */
        if (inner_project_rel != nullptr)
        {
            std::vector<Expr *> raw_virtuals;
            for (int i = 0; i < inner_project_rel->expressions_size(); i++)
            {
                raw_virtuals.push_back(convert_expression(
                    inner_project_rel->expressions(i),
                    projected_schema, func_map,
                    {}, nullptr, rel_cache, depth));
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
                        int vidx = src - n_input;
                        if (vidx < 0 || vidx >= (int)raw_virtuals.size())
                            throw SubstraitError(sfmt(
                                "substrait: emit mapping index %d out of range "
                                "(input=%d, virtuals=%d)",
                                src, n_input, (int)raw_virtuals.size()));
                        Expr *ve = raw_virtuals[vidx];
                        new_schema.push_back({0, 0, exprType((Node *)ve), nullptr});
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
                    projected_schema.push_back({0, 0, exprType((Node *)ve), nullptr});
                    virtual_map[n_input + (int)i] = ve;
                }
            }
        }
    }

    /* (filter for non-base case is now applied above, before inner_project.) */

    /* --- Aggregate --- */
    SubstraitSchema project_input_schema = projected_schema;
    VirtualMap outer_virtuals;

    if (agg_rel != nullptr)
    {
        query->targetList = aggregate_rel_to_targetlist(
            *agg_rel, query, projected_schema, func_map, virtual_map,
            rel_cache, depth);

        project_input_schema.clear();
        ListCell *lc;
        int idx = 0;
        foreach(lc, query->targetList)
        {
            TargetEntry *tle = (TargetEntry *)lfirst(lc);
            Oid otype = exprType((Node *)tle->expr);
            project_input_schema.push_back({0, 0, otype, nullptr});
            outer_virtuals[idx++] = tle->expr;
        }
    }
    else if (!schema.empty())
    {
        /* No aggregate — build tlist from full schema (all tables). */
        List *tlist = NIL;
        AttrNumber resno = 1;
        for (auto &[rt, attnum, typeoid, nullingrels] : schema)
        {
            Oid collation = is_string_type(typeoid)
                ? DEFAULT_COLLATION_OID : InvalidOid;
            Var *var = makeVar(rt, attnum, typeoid, -1, collation, 0);
            RangeTblEntry *rte =
                (RangeTblEntry *)list_nth(query->rtable, rt - 1);
            char *colname = (rte->rtekind == RTE_RELATION)
                ? get_attname(rte->relid, attnum, false)
                : pstrdup("?column?");
            TargetEntry *tle = makeTargetEntry(
                (Expr *)var, resno++, colname, false);
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
                func_map, outer_virtuals, nullptr, rel_cache, depth);

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

        /* WindowFunc fixup BEFORE emit: original pointers still match
         * g_pending_wins, so winref gets set on the originals. Then
         * copyObjectImpl in emit propagates the assigned winref. */
        fixup_window_clauses(query);

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
            /* Preserve groupClause-referenced TLEs dropped by emit.
             * PG requires grouped columns in targetList (resjunk=true if
             * not in SELECT). Without them, get_sortgroupref_tle fails. */
            {
                std::unordered_set<Index> present;
                ListCell *lc;
                foreach(lc, new_tlist)
                {
                    TargetEntry *tle = (TargetEntry *)lfirst(lc);
                    if (tle->ressortgroupref > 0)
                        present.insert(tle->ressortgroupref);
                }
                foreach(lc, old_tlist)
                {
                    TargetEntry *tle = (TargetEntry *)lfirst(lc);
                    if (tle->ressortgroupref > 0 &&
                        !present.count(tle->ressortgroupref))
                    {
                        TargetEntry *junk = (TargetEntry *)copyObjectImpl(tle);
                        junk->resno = resno++;
                        junk->resjunk = true;
                        new_tlist = lappend(new_tlist, junk);
                    }
                }
            }

            query->targetList = new_tlist;
        }
    }

    /* --- WindowFunc fixup: create WindowClause entries --- */
    fixup_window_clauses(query);

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
                throw SubstraitError("substrait: non-field sort expression");
            int field_idx = sf.expr().selection()
                                .direct_reference().struct_field().field();

            if (field_idx < 0 || field_idx >= list_length(query->targetList))
                throw SubstraitError(sfmt("substrait: sort field %d out of range",
                                          field_idx));
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

    /* Decorrelation disabled: PG's native SubPlan with correlated index
     * lookups is typically faster than the full-table HashAggregate that
     * pullup_correlated_scalar_sublinks produces (Q17: 33x regression). */
    // pullup_correlated_scalar_sublinks(query);

    /* Flag SubLinks so the planner runs SS_process_sublinks. */
    if (contains_sublink(query->jointree->quals) ||
        contains_sublink((Node *)query->targetList))
        query->hasSubLinks = true;

    /* CTE dedup: first occurrence of a duplicate subtree → save as CTE. */
    if (is_dup)
    {
        std::string cte_name = sfmt("_dedup_cte_%d", outer_cache->next_id++);

        CommonTableExpr *cte = makeNode(CommonTableExpr);
        cte->ctename = pstrdup(cte_name.c_str());
        cte->ctematerialized = CTEMaterializeAlways;
        cte->ctequery = (Node *)query;
        cte->cterefcount = 1;
        cte->ctecolnames = NIL;
        cte->ctecoltypes = NIL;
        cte->ctecoltypmods = NIL;
        cte->ctecolcollations = NIL;

        RelCache::CteEntry entry;
        entry.cte = cte;
        entry.cte_query = query;
        entry.col_names = NIL;

        ListCell *lc;
        foreach(lc, query->targetList)
        {
            TargetEntry *tle = (TargetEntry *)lfirst(lc);
            Oid t = exprType((Node *)tle->expr);
            int32 tm = exprTypmod((Node *)tle->expr);
            Oid coll = is_string_type(t)
                ? DEFAULT_COLLATION_OID : InvalidOid;

            const char *colname = tle->resname ? tle->resname : "?column?";
            entry.col_names = lappend(entry.col_names,
                                      makeString(pstrdup(colname)));
            entry.col_types.push_back(t);
            entry.col_typmods.push_back(tm);

            cte->ctecolnames = lappend(cte->ctecolnames,
                                       makeString(pstrdup(colname)));
            cte->ctecoltypes = lappend_oid(cte->ctecoltypes, t);
            cte->ctecoltypmods = lappend_int(cte->ctecoltypmods, tm);
            cte->ctecolcollations = lappend_oid(cte->ctecolcollations, coll);
        }

        outer_cache->cte_list = lappend(outer_cache->cte_list, cte);
        outer_cache->built[fp] = entry;

        return build_cte_ref_query(entry, depth);
    }

    return query;
}

static Query *
substrait_to_query_impl(const uint8_t *data, size_t len)
{
    g_pending_wins.clear();

    substrait::Plan plan;
    static constexpr size_t MAX_PLAN_SIZE = 64UL * 1024 * 1024;
    if (len > MAX_PLAN_SIZE || len > (size_t)INT_MAX)
        throw SubstraitError("substrait: plan too large");
    if (!plan.ParseFromArray(data, (int)len))
        throw SubstraitError("substrait: failed to deserialize protobuf Plan");

    if (plan.relations_size() == 0)
        throw SubstraitError("substrait: plan contains no relations");

    FuncMap func_map = build_func_map(plan);

    const substrait::PlanRel &prel = plan.relations(0);
    const substrait::Rel &root_rel =
        prel.has_root() ? prel.root().input() : prel.rel();

    const substrait::RelRoot *root =
        prel.has_root() ? &prel.root() : nullptr;

    /* Pre-scan: find duplicate Rel subtrees worth deduplicating. */
    RelCache rel_cache;
    {
#ifdef AFS_DEBUG
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
#endif
        count_rel_subtrees(root_rel, rel_cache);
#ifdef AFS_DEBUG
        clock_gettime(CLOCK_MONOTONIC, &t1);
        elog(LOG, "AFS_DEBUG: count_rel_subtrees %.3f ms, %zu dups",
             (t1.tv_sec - t0.tv_sec) * 1e3 + (t1.tv_nsec - t0.tv_nsec) / 1e6,
             rel_cache.duplicates.size());
#endif
    }

    ereport(NOTICE, (errmsg("substrait: %zu duplicate fingerprints (threshold 256 bytes)",
                             rel_cache.duplicates.size())));

#ifdef AFS_DEBUG
    struct timespec tb0, tb1;
    clock_gettime(CLOCK_MONOTONIC, &tb0);
#endif

    Query *query = build_query_for_rel(&root_rel, func_map, root,
                                        nullptr, &rel_cache, 0);

#ifdef AFS_DEBUG
    clock_gettime(CLOCK_MONOTONIC, &tb1);
    elog(LOG, "AFS_DEBUG: build_query_for_rel %.3f ms",
         (tb1.tv_sec - tb0.tv_sec) * 1e3 + (tb1.tv_nsec - tb0.tv_nsec) / 1e6);
#endif

    ereport(NOTICE, (errmsg("substrait: converter done, %d RTEs",
                             list_length(query->rtable))));

    /* Attach accumulated CTEs to the root query. */
    if (rel_cache.cte_list != NIL)
    {
        query->cteList = rel_cache.cte_list;
        query->hasModifyingCTE = false;
    }

    return query;
}

extern "C" Query *
substrait_to_query(const uint8_t *data, size_t len)
{
    try {
        return substrait_to_query_impl(data, len);
    } catch (const SubstraitError &e) {
        ereport(ERROR, (errmsg("%s", e.what())));
    } catch (const std::exception &e) {
        ereport(ERROR, (errmsg("substrait: %s", e.what())));
    }
    pg_unreachable();
}
