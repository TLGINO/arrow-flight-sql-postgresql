/*
 * Arrow Flight SQL Foreign Data Wrapper for PostgreSQL
 *
 * Enables CREATE FOREIGN TABLE backed by remote Flight SQL servers.
 * Usage:
 *   CREATE EXTENSION arrow_flight_sql_fdw;
 *   CREATE SERVER remote FOREIGN DATA WRAPPER arrow_flight_sql
 *     OPTIONS (uri 'grpc://host:15432');
 *   CREATE USER MAPPING FOR CURRENT_USER SERVER remote
 *     OPTIONS (username 'user', password 'pass');
 *   CREATE FOREIGN TABLE t1 (id int, name text)
 *     SERVER remote OPTIONS (table_name 'public.t1');
 */

extern "C" {
#include "postgres.h"
#include "fmgr.h"

#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "nodes/plannodes.h"
#include "miscadmin.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
}

#include "flight_client.h"

#include <arrow/array.h>
#include <arrow/scalar.h>
#include <arrow/type.h>

extern "C" {
/* FDW handler — called by CREATE FOREIGN DATA WRAPPER */
PG_FUNCTION_INFO_V1(arrow_flight_sql_fdw_handler);
PG_FUNCTION_INFO_V1(arrow_flight_sql_fdw_validator);
}

/* Per-scan state attached to ForeignScanState->fdw_state */
struct ArrowFdwState {
	std::shared_ptr<RemoteFlightConnection> conn;
	std::shared_ptr<arrow::RecordBatch> currentBatch;
	int64_t batchRow;  /* current row within currentBatch */
	bool queryStarted;
	std::string remoteQuery;
};

/* ---- Option helpers ---- */

static const char*
get_option(List* options, const char* name, const char* defval)
{
	ListCell* lc;
	foreach (lc, options)
	{
		auto def = (DefElem*)lfirst(lc);
		if (strcmp(def->defname, name) == 0)
			return defGetString(def);
	}
	return defval;
}

/* ---- Arrow → PG Datum conversion ---- */

static Datum
arrow_value_to_datum(const std::shared_ptr<arrow::Array>& arr,
                     int64_t row, Oid pg_type, bool* isnull)
{
	if (arr->IsNull(row))
	{
		*isnull = true;
		return (Datum)0;
	}
	*isnull = false;

	switch (arr->type_id())
	{
		case arrow::Type::BOOL:
		{
			auto a = std::static_pointer_cast<arrow::BooleanArray>(arr);
			return BoolGetDatum(a->Value(row));
		}
		case arrow::Type::INT16:
		{
			auto a = std::static_pointer_cast<arrow::Int16Array>(arr);
			return Int16GetDatum(a->Value(row));
		}
		case arrow::Type::INT32:
		{
			auto a = std::static_pointer_cast<arrow::Int32Array>(arr);
			return Int32GetDatum(a->Value(row));
		}
		case arrow::Type::INT64:
		{
			auto a = std::static_pointer_cast<arrow::Int64Array>(arr);
			return Int64GetDatum(a->Value(row));
		}
		case arrow::Type::FLOAT:
		{
			auto a = std::static_pointer_cast<arrow::FloatArray>(arr);
			return Float4GetDatum(a->Value(row));
		}
		case arrow::Type::DOUBLE:
		{
			auto a = std::static_pointer_cast<arrow::DoubleArray>(arr);
			return Float8GetDatum(a->Value(row));
		}
		case arrow::Type::STRING:
		{
			auto a = std::static_pointer_cast<arrow::StringArray>(arr);
			auto sv = a->GetView(row);
			auto len = sv.length();
			auto t = (text*)palloc(VARHDRSZ + len);
			SET_VARSIZE(t, VARHDRSZ + len);
			memcpy(VARDATA(t), sv.data(), len);
			return PointerGetDatum(t);
		}
		case arrow::Type::LARGE_STRING:
		{
			auto a = std::static_pointer_cast<arrow::LargeStringArray>(arr);
			auto sv = a->GetView(row);
			auto len = sv.length();
			auto t = (text*)palloc(VARHDRSZ + len);
			SET_VARSIZE(t, VARHDRSZ + len);
			memcpy(VARDATA(t), sv.data(), len);
			return PointerGetDatum(t);
		}
		case arrow::Type::DATE32:
		{
			auto a = std::static_pointer_cast<arrow::Date32Array>(arr);
			/* Arrow: days since 1970-01-01, PG: days since 2000-01-01 */
			int32_t days = a->Value(row) - 10957; /* 2000-01-01 - 1970-01-01 */
			return Int32GetDatum(days);
		}
		case arrow::Type::TIMESTAMP:
		{
			auto a = std::static_pointer_cast<arrow::TimestampArray>(arr);
			auto dt = std::static_pointer_cast<arrow::TimestampType>(arr->type());
			int64_t val = a->Value(row);
			/* Convert to PG epoch (microseconds since 2000-01-01) */
			int64_t pg_epoch_offset_us = INT64CONST(946684800000000); /* 2000-01-01 */
			switch (dt->unit())
			{
				case arrow::TimeUnit::SECOND:
					val = val * 1000000 - pg_epoch_offset_us;
					break;
				case arrow::TimeUnit::MILLI:
					val = val * 1000 - pg_epoch_offset_us;
					break;
				case arrow::TimeUnit::MICRO:
					val = val - pg_epoch_offset_us;
					break;
				case arrow::TimeUnit::NANO:
					val = val / 1000 - pg_epoch_offset_us;
					break;
			}
			return Int64GetDatum(val);
		}
		default:
		{
			/* Fallback: convert to text */
			auto result = arr->GetScalar(row);
			if (!result.ok())
			{
				*isnull = true;
				return (Datum)0;
			}
			auto str = (*result)->ToString();
			auto t = cstring_to_text_with_len(str.c_str(), str.size());
			return PointerGetDatum(t);
		}
	}
}

/* ---- FDW callbacks ---- */

static void
arrowGetForeignRelSize(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid)
{
	/* Conservative estimate — remote server may have any number of rows */
	baserel->rows = 1000;
}

static void
arrowGetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid)
{
	/* Single sequential scan path. Cost = startup + per-row. */
	Cost startup_cost = 25;  /* connection + query overhead */
	Cost total_cost = startup_cost + baserel->rows * 0.01;
	add_path(baserel,
	         (Path*)create_foreignscan_path(root, baserel,
	                                        NULL,  /* default pathtarget */
	                                        baserel->rows,
	                                        0,     /* disabled_nodes */
	                                        startup_cost,
	                                        total_cost,
	                                        NIL,   /* no pathkeys */
	                                        baserel->lateral_relids,
	                                        NULL,  /* no extra plan */
	                                        NIL,   /* no fdw_restrictinfo */
	                                        NIL)); /* no fdw_private */
}

static ForeignScan*
arrowGetForeignPlan(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid,
                    ForeignPath* best_path, List* tlist, List* scan_clauses,
                    Plan* outer_plan)
{
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Build remote query: SELECT col1, col2, ... FROM table_name */
	auto foreignTable = GetForeignTable(foreigntableid);
	auto table_name = get_option(foreignTable->options, "table_name", NULL);
	auto query_override = get_option(foreignTable->options, "query", NULL);

	char* remote_query;
	if (query_override)
	{
		remote_query = pstrdup(query_override);
	}
	else if (table_name)
	{
		/* Build SELECT with actual column names */
		Relation rel = table_open(foreigntableid, NoLock);
		TupleDesc tupdesc = RelationGetDescr(rel);
		StringInfoData buf;
		initStringInfo(&buf);
		appendStringInfo(&buf, "SELECT ");
		bool first = true;
		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);
			if (att->attisdropped) continue;
			if (!first) appendStringInfo(&buf, ", ");
			first = false;
			appendStringInfo(&buf, "\"%s\"", NameStr(att->attname));
		}
		appendStringInfo(&buf, " FROM %s", table_name);
		remote_query = buf.data;
		table_close(rel, NoLock);
	}
	else
	{
		elog(ERROR, "arrow_flight_sql FDW: table_name or query option required");
		return NULL;
	}

	/* Store remote query in fdw_private as a string constant */
	List* fdw_private = list_make1(makeString(remote_query));

	return make_foreignscan(tlist, scan_clauses, baserel->relid,
	                        NIL, /* no expressions for executor */
	                        fdw_private,
	                        NIL, /* no custom tlist */
	                        NIL, /* no remote quals */
	                        outer_plan);
}

static void
arrowBeginForeignScan(ForeignScanState* node, int eflags)
{
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	auto scan = (ForeignScan*)node->ss.ps.plan;
	auto remote_query = strVal(linitial(scan->fdw_private));

	/* Get server options */
	auto foreignTable = GetForeignTable(
		RelationGetRelid(node->ss.ss_currentRelation));
	auto server = GetForeignServer(foreignTable->serverid);
	auto uri = get_option(server->options, "uri", "grpc://127.0.0.1:15432");

	/* Get user mapping options */
	const char* username = "";
	const char* password = "";
	auto mapping = GetUserMapping(GetUserId(), server->serverid);
	if (mapping)
	{
		username = get_option(mapping->options, "username", "");
		password = get_option(mapping->options, "password", "");
	}

	auto database = get_option(server->options, "database", "");

	/* Create state */
	auto state = new ArrowFdwState();
	state->remoteQuery = remote_query;
	state->batchRow = 0;
	state->queryStarted = false;

	/* Connect — fresh connection per scan to avoid stale gRPC state */
	state->conn = std::make_shared<RemoteFlightConnection>(
		uri, database, username, password);
	auto connectStatus = state->conn->Connect();
	if (!connectStatus.ok())
	{
		delete state;
		elog(ERROR, "arrow_flight_sql FDW: connection failed: %s",
		     connectStatus.ToString().c_str());
		return;
	}

	auto status = state->conn->BeginQuery(state->remoteQuery);
	if (!status.ok())
	{
		delete state;
		elog(ERROR, "arrow_flight_sql FDW: query failed: %s",
		     status.ToString().c_str());
		return;
	}
	state->queryStarted = true;

	node->fdw_state = state;
}

static TupleTableSlot*
arrowIterateForeignScan(ForeignScanState* node)
{
	auto state = static_cast<ArrowFdwState*>(node->fdw_state);
	if (!state)
		return NULL;

	auto slot = node->ss.ss_ScanTupleSlot;
	ExecClearTuple(slot);

	/* Need a new batch? */
	while (!state->currentBatch ||
	       state->batchRow >= state->currentBatch->num_rows())
	{
		auto result = state->conn->NextBatch();
		if (!result.ok())
		{
			elog(WARNING, "arrow_flight_sql FDW: read error: %s",
			     result.status().ToString().c_str());
			return NULL;
		}
		state->currentBatch = *result;
		state->batchRow = 0;
		if (!state->currentBatch)
			return NULL;  /* done */
	}

	auto batch = state->currentBatch;
	int natts = slot->tts_tupleDescriptor->natts;

	for (int i = 0; i < natts && i < batch->num_columns(); i++)
	{
		auto att = TupleDescAttr(slot->tts_tupleDescriptor, i);
		if (att->attisdropped)
		{
			slot->tts_isnull[i] = true;
			continue;
		}
		bool isnull;
		slot->tts_values[i] = arrow_value_to_datum(
			batch->column(i), state->batchRow, att->atttypid, &isnull);
		slot->tts_isnull[i] = isnull;
	}

	state->batchRow++;
	ExecStoreVirtualTuple(slot);
	return slot;
}

static void
arrowReScanForeignScan(ForeignScanState* node)
{
	auto state = static_cast<ArrowFdwState*>(node->fdw_state);
	if (!state) return;

	/* Re-execute the query from scratch */
	state->conn->EndQuery();
	state->currentBatch.reset();
	state->batchRow = 0;

	auto status = state->conn->BeginQuery(state->remoteQuery);
	if (!status.ok())
	{
		elog(ERROR, "arrow_flight_sql FDW: rescan failed: %s",
		     status.ToString().c_str());
	}
	state->queryStarted = true;
}

static void
arrowEndForeignScan(ForeignScanState* node)
{
	auto state = static_cast<ArrowFdwState*>(node->fdw_state);
	if (!state) return;

	if (state->queryStarted)
		state->conn->EndQuery();
	delete state;
	node->fdw_state = NULL;
}

static void
arrowExplainForeignScan(ForeignScanState* node, ExplainState* es)
{
	auto scan = (ForeignScan*)node->ss.ps.plan;
	if (scan->fdw_private)
	{
		auto remote_query = strVal(linitial(scan->fdw_private));
		ExplainPropertyText("Remote Query", remote_query, es);
	}

	auto foreignTable = GetForeignTable(
		RelationGetRelid(node->ss.ss_currentRelation));
	auto server = GetForeignServer(foreignTable->serverid);
	auto uri = get_option(server->options, "uri", "grpc://127.0.0.1:15432");
	ExplainPropertyText("Flight SQL Server", uri, es);
}

/* ---- PG Datum → SQL literal ---- */

static std::string
datum_to_sql_literal(Datum value, Oid typid, bool isnull)
{
	if (isnull)
		return "NULL";

	bool typIsVarlena;
	Oid typOutput;
	getTypeOutputInfo(typid, &typOutput, &typIsVarlena);
	char* str = OidOutputFunctionCall(typOutput, value);

	/* Types that need quoting */
	switch (typid)
	{
		case BOOLOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
			if (strcmp(str, "Infinity") == 0)
				return "'Infinity'::double precision";
			if (strcmp(str, "-Infinity") == 0)
				return "'-Infinity'::double precision";
			if (strcmp(str, "NaN") == 0)
				return "'NaN'::double precision";
			return std::string(str);
		case NUMERICOID:
			return std::string(str);
		default:
		{
			/* Quote as string literal, escape single quotes */
			std::string escaped = "'";
			for (const char* p = str; *p; p++)
			{
				if (*p == '\'') escaped += "''";
				else escaped += *p;
			}
			escaped += "'";
			return escaped;
		}
	}
}

/* ---- FDW modify callbacks ---- */

struct ArrowFdwModifyState {
	std::shared_ptr<RemoteFlightConnection> conn;
	std::string table_name;
	std::vector<std::string> colNames;
	std::vector<Oid> colTypes;
	std::vector<std::string> pendingRows;
	int batchSize;
};

static void
arrowFlushInsertBatch(ArrowFdwModifyState* state)
{
	if (state->pendingRows.empty())
		return;

	/* Build: INSERT INTO table_name (col1,col2,...) VALUES (r1),(r2),... */
	std::string escaped_table;
	for (char c : state->table_name)
	{
		if (c == '"') escaped_table += "\"\"";
		else escaped_table += c;
	}
	std::string sql = "INSERT INTO \"" + escaped_table + "\" (";
	for (size_t i = 0; i < state->colNames.size(); i++)
	{
		if (i > 0) sql += ", ";
		/* Escape identifier */
		std::string escaped;
		for (char c : state->colNames[i])
		{
			if (c == '"') escaped += "\"\"";
			else escaped += c;
		}
		sql += "\"" + escaped + "\"";
	}
	sql += ") VALUES ";
	for (size_t i = 0; i < state->pendingRows.size(); i++)
	{
		if (i > 0) sql += ", ";
		sql += state->pendingRows[i];
	}

	auto result = state->conn->ExecuteUpdate(sql);
	state->pendingRows.clear();
	if (!result.ok())
	{
		elog(ERROR, "arrow_flight_sql FDW: INSERT failed: %s",
		     result.status().ToString().c_str());
	}
}

static List*
arrowPlanForeignModify(PlannerInfo* root, ModifyTable* plan,
                       Index resultRelation, int subplan_index)
{
	if (plan->operation != CMD_INSERT)
		elog(ERROR, "arrow_flight_sql FDW: only INSERT is supported for writes");

	RangeTblEntry* rte = root->simple_rte_array
		? root->simple_rte_array[resultRelation]
		: (RangeTblEntry*)list_nth(root->parse->rtable, resultRelation - 1);
	Relation rel = table_open(rte->relid, NoLock);
	TupleDesc tupdesc = RelationGetDescr(rel);

	List* colNames = NIL;
	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		if (att->attisdropped) continue;
		colNames = lappend(colNames, makeString(pstrdup(NameStr(att->attname))));
	}
	table_close(rel, NoLock);

	return colNames;
}

static void
arrowBeginForeignModify(ModifyTableState* mtstate, ResultRelInfo* rinfo,
                        List* fdw_private, int subplan_index, int eflags)
{
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	auto foreignTable = GetForeignTable(
		RelationGetRelid(rinfo->ri_RelationDesc));
	auto server = GetForeignServer(foreignTable->serverid);
	auto uri = get_option(server->options, "uri", "grpc://127.0.0.1:15432");
	auto database = get_option(server->options, "database", "");
	auto table_name = get_option(foreignTable->options, "table_name", NULL);
	if (!table_name)
		elog(ERROR, "arrow_flight_sql FDW: table_name required for INSERT");

	const char* username = "";
	const char* password = "";
	auto mapping = GetUserMapping(GetUserId(), server->serverid);
	if (mapping)
	{
		username = get_option(mapping->options, "username", "");
		password = get_option(mapping->options, "password", "");
	}

	auto state = new ArrowFdwModifyState();
	state->table_name = table_name;
	state->batchSize = 100;

	/* Extract column names and types from fdw_private */
	TupleDesc tupdesc = RelationGetDescr(rinfo->ri_RelationDesc);
	ListCell* lc;
	int i = 0;
	foreach (lc, fdw_private)
	{
		state->colNames.push_back(strVal(lfirst(lc)));
		/* Find the matching non-dropped attribute for the type */
		while (i < tupdesc->natts &&
		       TupleDescAttr(tupdesc, i)->attisdropped)
			i++;
		if (i < tupdesc->natts)
		{
			state->colTypes.push_back(TupleDescAttr(tupdesc, i)->atttypid);
			i++;
		}
	}

	state->conn = std::make_shared<RemoteFlightConnection>(
		uri, database, username, password);
	auto status = state->conn->Connect();
	if (!status.ok())
	{
		delete state;
		elog(ERROR, "arrow_flight_sql FDW: connection failed: %s",
		     status.ToString().c_str());
	}

	rinfo->ri_FdwState = state;
}

static TupleTableSlot*
arrowExecForeignInsert(EState* estate, ResultRelInfo* rinfo,
                       TupleTableSlot* slot, TupleTableSlot* planSlot)
{
	auto state = static_cast<ArrowFdwModifyState*>(rinfo->ri_FdwState);

	slot_getallattrs(slot);

	std::string row = "(";
	int col = 0;
	for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);
		if (att->attisdropped) continue;
		if (col > 0) row += ", ";
		row += datum_to_sql_literal(
			slot->tts_values[i], att->atttypid, slot->tts_isnull[i]);
		col++;
	}
	row += ")";

	state->pendingRows.push_back(std::move(row));
	if ((int)state->pendingRows.size() >= state->batchSize)
		arrowFlushInsertBatch(state);

	return slot;
}

static void
arrowEndForeignModify(EState* estate, ResultRelInfo* rinfo)
{
	auto state = static_cast<ArrowFdwModifyState*>(rinfo->ri_FdwState);
	if (!state) return;

	arrowFlushInsertBatch(state);
	state->conn->Close();
	delete state;
	rinfo->ri_FdwState = NULL;
}

/* ---- FDW handler entry points ---- */

extern "C" Datum
arrow_flight_sql_fdw_handler(PG_FUNCTION_ARGS)
{
	auto routine = makeNode(FdwRoutine);

	/* Scan */
	routine->GetForeignRelSize = arrowGetForeignRelSize;
	routine->GetForeignPaths = arrowGetForeignPaths;
	routine->GetForeignPlan = arrowGetForeignPlan;
	routine->BeginForeignScan = arrowBeginForeignScan;
	routine->IterateForeignScan = arrowIterateForeignScan;
	routine->ReScanForeignScan = arrowReScanForeignScan;
	routine->EndForeignScan = arrowEndForeignScan;
	routine->ExplainForeignScan = arrowExplainForeignScan;

	/* Modify (INSERT) */
	routine->PlanForeignModify = arrowPlanForeignModify;
	routine->BeginForeignModify = arrowBeginForeignModify;
	routine->ExecForeignInsert = arrowExecForeignInsert;
	routine->EndForeignModify = arrowEndForeignModify;

	PG_RETURN_POINTER(routine);
}

extern "C" Datum
arrow_flight_sql_fdw_validator(PG_FUNCTION_ARGS)
{
	/* Accept all options for now */
	PG_RETURN_VOID();
}
