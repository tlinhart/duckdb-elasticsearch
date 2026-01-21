#pragma once

#include "duckdb.hpp"
#include "duckdb/logging/logger.hpp"
#include <string>
#include <vector>
#include <memory>

namespace duckdb {

struct ElasticsearchConfig {
	std::string host = "localhost";    // Elasticsearch host (hostname or IP)
	int32_t port = 9200;               // Elasticsearch port
	std::string username;              // optional username for HTTP basic authentication
	std::string password;              // optional password for HTTP basic authentication
	bool use_ssl = false;              // whether to use HTTPS instead of HTTP
	bool verify_ssl = true;            // whether to verify SSL certificates
	int32_t timeout = 30000;           // request timeout in milliseconds
	int32_t max_retries = 3;           // maximum number of retries for transient errors
	int32_t retry_interval = 100;      // initial wait time between retries in milliseconds
	double retry_backoff_factor = 2.0; // exponential backoff factor applied between retries
};

struct ElasticsearchResponse {
	bool success;
	int32_t status_code;
	std::string body;
	std::string error_message;
};

class ElasticsearchClient {
public:
	explicit ElasticsearchClient(const ElasticsearchConfig &config, shared_ptr<Logger> logger = nullptr);
	~ElasticsearchClient();

	// Scroll API for large result sets.
	ElasticsearchResponse ScrollSearch(const std::string &index, const std::string &query,
	                                   const std::string &scroll_time = "1m", int64_t size = 1000);
	ElasticsearchResponse ScrollNext(const std::string &scroll_id, const std::string &scroll_time = "1m");
	ElasticsearchResponse ClearScroll(const std::string &scroll_id);

	// Get index mapping.
	ElasticsearchResponse GetMapping(const std::string &index);

private:
	ElasticsearchConfig config_;
	shared_ptr<Logger> logger_;

	// Perform HTTP request using httplib with OpenSSL.
	ElasticsearchResponse PerformRequest(const std::string &method, const std::string &path,
	                                     const std::string &body = "");

	// Perform request with retry logic for transient errors.
	ElasticsearchResponse PerformRequestWithRetry(const std::string &method, const std::string &path,
	                                              const std::string &body = "");
};

} // namespace duckdb
