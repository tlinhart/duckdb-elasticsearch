#include "es_client.hpp"
#include "duckdb/common/exception.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

#include <chrono>
#include <thread>
#include <unordered_set>

namespace duckdb {

// HTTP status codes that indicate transient errors which should be retried.
static const std::unordered_set<int> RETRYABLE_STATUS_CODES = {
    429, // Too Many Requests
    500, // Internal Server Error
    502, // Bad Gateway
    503, // Service Unavailable
    504  // Gateway Timeout
};

ElasticsearchClient::ElasticsearchClient(const ElasticsearchConfig &config) : config_(config) {
}

ElasticsearchClient::~ElasticsearchClient() {
}

ElasticsearchResponse ElasticsearchClient::PerformRequest(const std::string &method, const std::string &path,
                                                          const std::string &body) {
	ElasticsearchResponse response;
	response.success = false;
	response.status_code = 0;

	try {
		// Create client with base URL.
		std::string protocol = config_.use_ssl ? "https" : "http";
		std::string base_url = protocol + "://" + config_.host + ":" + std::to_string(config_.port);
		duckdb_httplib_openssl::Client client(base_url);

		// Configure timeouts.
		int timeout_sec = config_.timeout / 1000;
		int timeout_usec = (config_.timeout % 1000) * 1000;
		client.set_read_timeout(timeout_sec, timeout_usec);
		client.set_write_timeout(timeout_sec, timeout_usec);
		client.set_connection_timeout(timeout_sec, timeout_usec);

		// Configure SSL verification.
		if (config_.use_ssl && !config_.verify_ssl) {
			client.enable_server_certificate_verification(false);
		}

		// Follow redirects.
		client.set_follow_location(true);

		// Set basic auth if credentials are provided.
		if (!config_.username.empty()) {
			client.set_basic_auth(config_.username, config_.password);
		}

		// Prepare headers.
		duckdb_httplib_openssl::Headers headers = {{"Accept", "application/json"}};

		// Make request based on method.
		duckdb_httplib_openssl::Result result;

		if (method == "GET") {
			result = client.Get(path, headers);
		} else if (method == "POST") {
			result = client.Post(path, headers, body, "application/json");
		} else if (method == "PUT") {
			result = client.Put(path, headers, body, "application/json");
		} else if (method == "DELETE") {
			result = client.Delete(path, headers, body, "application/json");
		} else {
			response.error_message = "Unsupported HTTP method: " + method;
			return response;
		}

		// Process response.
		if (result) {
			response.status_code = result->status;
			response.body = result->body;
			response.success = (result->status >= 200 && result->status < 300);

			if (!response.success) {
				response.error_message = "HTTP " + std::to_string(result->status) + ": " + result->body;
			}
		} else {
			std::string error_message = duckdb_httplib_openssl::to_string(result.error());
			response.error_message = "HTTP " + method + " request failed: " + error_message;
		}

	} catch (const std::exception &e) {
		response.error_message = std::string("Exception during HTTP request: ") + e.what();
	}

	return response;
}

ElasticsearchResponse ElasticsearchClient::PerformRequestWithRetry(const std::string &method, const std::string &path,
                                                                   const std::string &body) {
	int retry_count = 0;
	double backoff_ms = static_cast<double>(config_.retry_interval);
	ElasticsearchResponse response;

	while (retry_count <= config_.max_retries) {
		response = PerformRequest(method, path, body);

		// If successful, return immediately.
		if (response.success) {
			return response;
		}

		// Check if we should retry.
		bool should_retry = false;

		if (response.status_code > 0) {
			// We got an HTTP response - check if status code is retryable.
			should_retry = RETRYABLE_STATUS_CODES.count(response.status_code) > 0;
		} else {
			// status_code of 0 typically means network errors which are generally retryable.
			should_retry = true;
		}

		if (!should_retry || retry_count >= config_.max_retries) {
			break;
		}

		// Wait before retrying with exponential backoff.
		std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(backoff_ms)));
		backoff_ms *= config_.retry_backoff_factor;
		retry_count++;
	}

	// Add retry information to error message if we exhausted retries.
	if (retry_count > 0 && !response.success) {
		response.error_message += " (after " + std::to_string(retry_count) + " retries)";
	}

	return response;
}

ElasticsearchResponse ElasticsearchClient::ScrollSearch(const std::string &index, const std::string &query,
                                                        const std::string &scroll_time, int64_t size) {
	std::string path = "/" + index + "/_search?scroll=" + scroll_time + "&size=" + std::to_string(size);
	return PerformRequestWithRetry("POST", path, query);
}

ElasticsearchResponse ElasticsearchClient::ScrollNext(const std::string &scroll_id, const std::string &scroll_time) {
	std::string body = R"({"scroll":")" + scroll_time + R"(","scroll_id":")" + scroll_id + R"("})";
	return PerformRequestWithRetry("POST", "/_search/scroll", body);
}

ElasticsearchResponse ElasticsearchClient::ClearScroll(const std::string &scroll_id) {
	std::string body = R"({"scroll_id":")" + scroll_id + R"("})";
	// Don't retry scroll cleanup as it's not critical if it fails.
	return PerformRequest("DELETE", "/_search/scroll", body);
}

ElasticsearchResponse ElasticsearchClient::GetMapping(const std::string &index) {
	return PerformRequestWithRetry("GET", "/" + index + "/_mapping", "");
}

ElasticsearchResponse ElasticsearchClient::Aggregate(const std::string &index, const std::string &agg_query) {
	std::string path = "/" + index + "/_search?size=0";
	return PerformRequestWithRetry("POST", path, agg_query);
}

} // namespace duckdb
