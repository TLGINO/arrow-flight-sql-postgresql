#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <arrow/flight/sql/client.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/table.h>

// Manages a connection to a remote Arrow Flight SQL server.
class RemoteFlightConnection {
   public:
	RemoteFlightConnection(std::string uri,
	                       std::string database,
	                       std::string user,
	                       std::string password)
		: uri_(std::move(uri)),
		  database_(std::move(database)),
		  user_(std::move(user)),
		  password_(std::move(password))
	{
	}

	~RemoteFlightConnection() { Close(); }

	// Connect and authenticate. Idempotent.
	arrow::Status Connect();

	// Start executing a query. Opens the reader for streaming results.
	arrow::Status BeginQuery(const std::string& query);

	// Get schema of the current query results.
	std::shared_ptr<arrow::Schema> schema() const;

	// Read next batch. Returns nullptr when done.
	arrow::Result<std::shared_ptr<arrow::RecordBatch>> NextBatch();

	// End the current query and release the reader.
	void EndQuery();

	// Execute a query and collect all results into a Table.
	arrow::Result<std::shared_ptr<arrow::Table>> ExecuteAndCollect(
		const std::string& query);

	// Execute a DML statement (INSERT/UPDATE/DELETE). Returns affected row count.
	arrow::Result<int64_t> ExecuteUpdate(const std::string& query);

	void Close();

	const std::string& uri() const { return uri_; }

   private:
	std::string uri_;
	std::string database_;
	std::string user_;
	std::string password_;
	std::unique_ptr<arrow::flight::sql::FlightSqlClient> client_;
	arrow::flight::FlightCallOptions call_options_;
	// Active query state
	std::unique_ptr<arrow::flight::FlightStreamReader> reader_;
	std::shared_ptr<arrow::Schema> schema_;
};

// Global connection pool: caches connections by URI.
class FlightClientPool {
   public:
	static FlightClientPool& instance()
	{
		static FlightClientPool pool;
		return pool;
	}

	arrow::Result<std::shared_ptr<RemoteFlightConnection>> Get(
		const std::string& uri,
		const std::string& database,
		const std::string& user,
		const std::string& password);

	void Remove(const std::string& uri);
	void Clear();

	~FlightClientPool() { Clear(); }

   private:
	FlightClientPool() = default;
	std::mutex mutex_;
	std::unordered_map<std::string, std::shared_ptr<RemoteFlightConnection>> pool_;
};
