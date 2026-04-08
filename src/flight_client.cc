#include "flight_client.h"

#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/table.h>

arrow::Status
RemoteFlightConnection::Connect()
{
	if (client_)
		return arrow::Status::OK();

	ARROW_ASSIGN_OR_RAISE(auto location, arrow::flight::Location::Parse(uri_));
	arrow::flight::FlightClientOptions client_options;
	ARROW_ASSIGN_OR_RAISE(auto flight_client,
	                       arrow::flight::FlightClient::Connect(location, client_options));

	if (!database_.empty())
	{
		call_options_.headers.emplace_back("x-flight-sql-database", database_);
	}

	if (!user_.empty())
	{
		ARROW_ASSIGN_OR_RAISE(
			auto bearer_token,
			flight_client->AuthenticateBasicToken(call_options_, user_, password_));
		if (!bearer_token.first.empty() && !bearer_token.second.empty())
		{
			call_options_.headers.emplace_back(bearer_token.first,
			                                   bearer_token.second);
		}
	}

	client_ = std::make_unique<arrow::flight::sql::FlightSqlClient>(
		std::move(flight_client));
	return arrow::Status::OK();
}

arrow::Status
RemoteFlightConnection::BeginQuery(const std::string& query)
{
	ARROW_RETURN_NOT_OK(Connect());

	ARROW_ASSIGN_OR_RAISE(auto info, client_->Execute(call_options_, query));
	const auto& endpoints = info->endpoints();
	if (endpoints.empty())
	{
		return arrow::Status::Invalid("No endpoints returned for query");
	}

	ARROW_ASSIGN_OR_RAISE(reader_,
	                       client_->DoGet(call_options_, endpoints[0].ticket));
	ARROW_ASSIGN_OR_RAISE(schema_, reader_->GetSchema());
	return arrow::Status::OK();
}

std::shared_ptr<arrow::Schema>
RemoteFlightConnection::schema() const
{
	return schema_;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
RemoteFlightConnection::NextBatch()
{
	if (!reader_)
		return arrow::Status::Invalid("No active query");

	arrow::flight::FlightStreamChunk chunk;
	ARROW_ASSIGN_OR_RAISE(chunk, reader_->Next());
	if (!chunk.data)
		return nullptr;
	return chunk.data;
}

void
RemoteFlightConnection::EndQuery()
{
	reader_.reset();
	schema_.reset();
}

arrow::Result<std::shared_ptr<arrow::Table>>
RemoteFlightConnection::ExecuteAndCollect(const std::string& query)
{
	ARROW_RETURN_NOT_OK(BeginQuery(query));
	auto saved_schema = schema_;
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
	while (true)
	{
		auto result = NextBatch();
		if (!result.ok())
		{
			EndQuery();
			return result.status();
		}
		auto batch = *result;
		if (!batch) break;
		batches.push_back(std::move(batch));
	}
	EndQuery();
	if (batches.empty())
		return arrow::Table::MakeEmpty(saved_schema ? saved_schema : arrow::schema({}));
	return arrow::Table::FromRecordBatches(batches);
}

arrow::Result<int64_t>
RemoteFlightConnection::ExecuteUpdate(const std::string& query)
{
	ARROW_RETURN_NOT_OK(Connect());
	return client_->ExecuteUpdate(call_options_, query);
}

void
RemoteFlightConnection::Close()
{
	EndQuery();
	if (client_)
	{
		arrow::flight::CloseSessionRequest req;
		(void)client_->CloseSession(call_options_, req);
		(void)client_->Close();
		client_.reset();
	}
	call_options_ = arrow::flight::FlightCallOptions();
}

// FlightClientPool

arrow::Result<std::shared_ptr<RemoteFlightConnection>>
FlightClientPool::Get(const std::string& uri,
                      const std::string& database,
                      const std::string& user,
                      const std::string& password)
{
	std::lock_guard<std::mutex> lock(mutex_);
	auto it = pool_.find(uri);
	if (it != pool_.end())
		return it->second;

	auto conn = std::make_shared<RemoteFlightConnection>(
		uri, database, user, password);
	ARROW_RETURN_NOT_OK(conn->Connect());
	pool_[uri] = conn;
	return conn;
}

void
FlightClientPool::Remove(const std::string& uri)
{
	std::lock_guard<std::mutex> lock(mutex_);
	auto it = pool_.find(uri);
	if (it != pool_.end())
	{
		it->second->Close();
		pool_.erase(it);
	}
}

void
FlightClientPool::Clear()
{
	std::lock_guard<std::mutex> lock(mutex_);
	for (auto& [_, conn] : pool_)
		conn->Close();
	pool_.clear();
}
