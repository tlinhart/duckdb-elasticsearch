#pragma once

#include "duckdb.hpp"
#include "duckdb/logging/logger.hpp"
#include <string>
#include <vector>
#include <memory>

typedef void CURL;
struct curl_slist;

namespace duckdb {

struct ElasticsearchConfig {
	std::string host;            // Elasticsearch host (hostname or IP)
	int32_t port;                // Elasticsearch port
	std::string username;        // optional username for HTTP basic authentication
	std::string password;        // optional password for HTTP basic authentication
	bool use_ssl;                // whether to use HTTPS instead of HTTP
	bool verify_ssl;             // whether to verify SSL certificates
	int32_t timeout;             // request timeout in milliseconds
	int32_t max_retries;         // maximum number of retries for transient errors
	int32_t retry_interval;      // initial wait time between retries in milliseconds
	double retry_backoff_factor; // exponential backoff factor applied between retries
	std::string proxy_host;      // HTTP proxy host
	std::string proxy_username;  // username for HTTP proxy
	std::string proxy_password;  // password for HTTP proxy
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

	// Disable copy (CURL handle is not copyable).
	ElasticsearchClient(const ElasticsearchClient &) = delete;
	ElasticsearchClient &operator=(const ElasticsearchClient &) = delete;

	// Plain search (no scroll context). Suitable for bounded result sets (e.g. sampling).
	ElasticsearchResponse Search(const std::string &index, const std::string &query, int64_t size);

	// Scroll API for large result sets.
	ElasticsearchResponse ScrollSearch(const std::string &index, const std::string &query,
	                                   const std::string &scroll_time, int64_t size);
	ElasticsearchResponse ScrollNext(const std::string &scroll_id, const std::string &scroll_time);
	ElasticsearchResponse ClearScroll(const std::string &scroll_id);

	// Get index mapping.
	ElasticsearchResponse GetMapping(const std::string &index);

private:
	ElasticsearchConfig config_;
	shared_ptr<Logger> logger_;
	CURL *curl_handle_;
	std::string base_url_;

	// Perform HTTP request using libcurl.
	ElasticsearchResponse PerformRequest(const std::string &method, const std::string &path,
	                                     const std::string &body = "");

	// Perform request with retry logic for transient errors.
	ElasticsearchResponse PerformRequestWithRetry(const std::string &method, const std::string &path,
	                                              const std::string &body = "");

	// Configure the CURL handle with common options (timeouts, SSL, auth, proxy).
	void ConfigureCurlHandle();
};

} // namespace duckdb
