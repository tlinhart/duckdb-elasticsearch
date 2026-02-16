#include "elasticsearch_client.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/logging/log_type.hpp"

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

// Format headers map as a string for logging.
static std::string FormatHeaders(const duckdb_httplib_openssl::Headers &headers) {
	std::string headers_str = "{";
	bool first = true;
	for (const auto &header : headers) {
		if (!first) {
			headers_str += ", ";
		}
		first = false;
		headers_str += header.first + "='" + header.second + "'";
	}
	headers_str += "}";
	return headers_str;
}

// Construct HTTP log message in the same format as DuckDB's HTTPLogType.
static std::string ConstructHTTPLogMessage(const duckdb_httplib_openssl::Request &request,
                                           const duckdb_httplib_openssl::Response &response,
                                           std::chrono::system_clock::time_point start_time,
                                           std::chrono::system_clock::time_point end_time) {
	// Format start time as string.
	auto start_time_t = std::chrono::system_clock::to_time_t(start_time);
	char start_time_str[64];
	std::strftime(start_time_str, sizeof(start_time_str), "%Y-%m-%d %H:%M:%S", std::gmtime(&start_time_t));

	// Calculate duration in milliseconds.
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

	// Build request part.
	std::string log_msg = "{'request': {'type': " + request.method + ", 'url': '" + request.path +
	                      "', 'headers': " + FormatHeaders(request.headers);

	// Only include body if it is not empty.
	if (!request.body.empty()) {
		log_msg += ", 'body': '" + request.body + "'";
	}

	log_msg += ", 'start_time': '" + std::string(start_time_str) + "', 'duration_ms': " + std::to_string(duration_ms) +
	           "}, 'response': ";

	// Build response part if we have a valid response.
	if (response.status != -1) {
		log_msg += "{'status': " + std::to_string(response.status) + ", 'reason': " + response.reason +
		           ", 'headers': " + FormatHeaders(response.headers) + "}";
	} else {
		log_msg += "NULL";
	}

	log_msg += "}";
	return log_msg;
}

ElasticsearchClient::ElasticsearchClient(const ElasticsearchConfig &config, shared_ptr<Logger> logger)
    : config_(config), logger_(std::move(logger)) {
}

ElasticsearchClient::~ElasticsearchClient() {
}

ElasticsearchResponse ElasticsearchClient::PerformRequest(const std::string &method, const std::string &path,
                                                          const std::string &body) {
	ElasticsearchResponse response;
	response.success = false;
	response.status_code = 0;

	// Record start time for logging.
	auto start_time = std::chrono::system_clock::now();

	// Variables to capture request and response details for logging.
	duckdb_httplib_openssl::Request logged_request;
	duckdb_httplib_openssl::Response logged_response;
	bool should_log = logger_ && logger_->ShouldLog(HTTPLogType::NAME, HTTPLogType::LEVEL);

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

		if (should_log) {
			// Set up httplib logger to capture actual request and response details.
			// Important: This callback is only invoked when requests complete successfully,
			// failed requests (connection errors, timeouts, SSL failures) are never logged.
			client.set_logger(
			    [&](const duckdb_httplib_openssl::Request &req, const duckdb_httplib_openssl::Response &resp) {
				    logged_request.method = req.method;
				    logged_request.path = req.path;
				    logged_request.headers = req.headers;
				    logged_request.body = req.body;
				    logged_response.status = resp.status;
				    logged_response.reason = resp.reason;
				    logged_response.headers = resp.headers;
			    });
		}

		// Make request.
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

	// Log the request after successful completion using details captured by httplib logger.
	if (should_log && response.status_code > 0) {
		auto end_time = std::chrono::system_clock::now();
		std::string log_msg = ConstructHTTPLogMessage(logged_request, logged_response, start_time, end_time);
		logger_->WriteLog(HTTPLogType::NAME, HTTPLogType::LEVEL, log_msg);
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
			// We got an HTTP response, check if status code is retryable.
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

ElasticsearchResponse ElasticsearchClient::Search(const std::string &index, const std::string &query, int64_t size) {
	std::string path = "/" + index + "/_search?size=" + std::to_string(size);
	return PerformRequestWithRetry("POST", path, query);
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
	// Do not retry scroll cleanup as it's not critical if it fails.
	return PerformRequest("DELETE", "/_search/scroll", body);
}

ElasticsearchResponse ElasticsearchClient::GetMapping(const std::string &index) {
	return PerformRequestWithRetry("GET", "/" + index + "/_mapping", "");
}

} // namespace duckdb
