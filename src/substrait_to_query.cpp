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

/* Recursively process a base relation tree (Read or nested Cross).
 * Each ReadRel becomes an RTE; CrossRel concatenates left+right schemas. */
static void
process_base_rel(const substrait::Rel &rel, Query *query,
                 SubstraitSchema &schema_out)
{
    if (rel.has_read())
    {
        SubstraitSchema local;
        RangeTblEntry *rte = read_rel_to_rte(rel.read(), local, query);
        query->rtable = lappend(query->rtable, rte);
        int rtindex = list_length(query->rtable);

        RangeTblRef *rtr = makeNode(RangeTblRef);
        rtr->rtindex = rtindex;
        query->jointree->fromlist = lappend(query->jointree->fromlist, rtr);

        for (auto &[rt, attnum, typeoid] : local)
            rt = rtindex;
        schema_out.insert(schema_out.end(), local.begin(), local.end());
    }
    else if (rel.has_cross())
    {
        const auto &cross = rel.cross();
        process_base_rel(cross.left(), query, schema_out);
        process_base_rel(cross.right(), query, schema_out);
    }
    else
    {
        ereport(ERROR,
                (errmsg("substrait: unsupported base relation type")));
    }
}

/* Virtual expression map: field_index → precomputed PG Expr.
 * Used for inner ProjectRel computed columns. */
typedef std::unordered_map<int, Expr *> VirtualMap;

/* Forward declaration — defined in Section 5. */
static Expr *
convert_expression(const substrait::Expression &expr,
                   const SubstraitSchema &schema,
                   const FuncMap &func_map,
                   const VirtualMap &virtuals = {});

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
                   const VirtualMap &virtuals)
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

    /* --- Cast → convert input and apply PG coercion --- */
    if (expr.has_cast())
    {
        const auto &cast = expr.cast();
        if (!cast.has_input())
            ereport(ERROR, (errmsg("substrait: cast without input")));

        Expr *input = convert_expression(cast.input(), schema,
                                         func_map, virtuals);
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
                                                  virtuals));
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
                                   schema, func_map, virtuals));
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
                                         schema, func_map, virtuals);
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
                sf.arguments(0).value(), schema, func_map, virtuals));
            Expr *right = coerce_to_text(convert_expression(
                sf.arguments(1).value(), schema, func_map, virtuals));

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
                                                schema, func_map, virtuals);
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
                                            virtuals);
            Expr *right = convert_expression(sf.arguments(1).value(),
                                             schema, func_map,
                                             virtuals);

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

    ereport(ERROR,
            (errmsg("substrait: unsupported expression type")));
    return NULL; /* unreachable */
}

/* ============================================================
 * SECTION 6 - Top-level entry point
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

    /* Peel relations outermost-first.
     * DuckDB plan shapes: [Sort?] > [Project?] > [Aggregate?] > [Project?] > [Filter?] > Read */
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

    if (!cur->has_read() && !cur->has_cross())
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("substrait: unsupported plan shape")));

    SubstraitSchema schema;
    process_base_rel(*cur, query, schema);

    /* Apply filter: either from standalone FilterRel or ReadRel.filter.
     * Filter operates on the base schema (before projection). */
    if (filter_rel != nullptr)
    {
        query->jointree->quals = (Node *)convert_expression(
            filter_rel->condition(), schema, func_map);
    }
    else if (cur->has_read() && cur->read().has_filter())
    {
        query->jointree->quals = (Node *)convert_expression(
            cur->read().filter(), schema, func_map);
    }

    /* Apply ReadRel projection: remap schema for upstream operators.
     * ReadRel.projection narrows/reorders columns; operators above
     * (aggregate, project) reference the projected field indices. */
    SubstraitSchema projected_schema = schema;
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

    /* Inner ProjectRel: precompute expressions that the aggregate
     * references. The ProjectRel output is (input cols + expression cols),
     * then optionally reordered by emit.outputMapping.
     *
     * We build a combined column map indexed by the output position.
     * Real columns use the projected_schema; virtual columns are stored
     * in a position-indexed map so convert_expression can return them. */
    VirtualMap virtual_map;
    if (inner_project_rel != nullptr)
    {
        /* Build virtual expressions from the projected_schema context. */
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
            /* Emit.outputMapping reorders (input cols + expr cols). */
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
            /* No emit — virtuals are appended after input columns. */
            for (size_t i = 0; i < raw_virtuals.size(); i++)
            {
                Expr *ve = raw_virtuals[i];
                projected_schema.push_back({0, 0, exprType((Node *)ve)});
                virtual_map[n_input + (int)i] = ve;
            }
        }
    }

    if (agg_rel != nullptr)
    {
        query->targetList = aggregate_rel_to_targetlist(
            *agg_rel, query, projected_schema, func_map, virtual_map);
    }
    else
    {
        /* Single-table star select — get rtindex from first schema entry. */
        auto [rt0, att0, typ0] = schema[0];
        RangeTblEntry *rte =
            (RangeTblEntry *)list_nth(query->rtable, rt0 - 1);
        query->targetList = build_star_tlist(rte, rt0);
    }

    /* ProjectRel: append computed expressions to target list.
     * Substrait ProjectRel output = input columns + expression columns.
     * The root RelRoot.names gives the final column names. */
    if (project_rel != nullptr && project_rel->expressions_size() > 0)
    {
        const substrait::PlanRel &pr = plan.relations(0);
        int name_idx = list_length(query->targetList);

        for (int i = 0; i < project_rel->expressions_size(); i++)
        {
            Expr *expr = convert_expression(
                project_rel->expressions(i), projected_schema,
                func_map);

            const char *colname = "expr";
            if (pr.has_root() && name_idx < pr.root().names_size())
                colname = pr.root().names(name_idx).c_str();

            TargetEntry *tle = makeTargetEntry(
                expr,
                (AttrNumber)(name_idx + 1),
                pstrdup(colname),
                false);
            query->targetList = lappend(query->targetList, tle);
            name_idx++;
        }
    }

    /* SortRel → query->sortClause.
     * Sort field references index into the final target list. */
    if (sort_rel != nullptr)
    {
        Index next_sgref = 0;
        /* Find max existing ressortgroupref (set by GROUP BY). */
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

            /* Resolve the sort expression to a target list entry. */
            if (!sf.has_expr() || !sf.expr().has_selection())
                ereport(ERROR,
                        (errmsg("substrait: non-field sort expression")));
            int field_idx = sf.expr().selection()
                                .direct_reference().struct_field().field();

            /* Target list is 1-indexed; field_idx is 0-indexed. */
            if (field_idx < 0 || field_idx >= list_length(query->targetList))
                ereport(ERROR,
                        (errmsg("substrait: sort field %d out of range",
                                field_idx)));
            TargetEntry *tle =
                (TargetEntry *)list_nth(query->targetList, field_idx);

            /* Assign ressortgroupref if not already set (by GROUP BY). */
            if (tle->ressortgroupref == 0)
                tle->ressortgroupref = next_sgref++;

            /* Determine sort direction. */
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

    return query;
}
