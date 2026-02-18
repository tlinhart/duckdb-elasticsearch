#include "elasticsearch_client.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/log_type.hpp"

#include <curl/curl.h>

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

// Callback for libcurl to write response body data.
static size_t WriteCallback(char *ptr, size_t size, size_t nmemb, void *userdata) {
	auto *response_body = static_cast<std::string *>(userdata);
	size_t total_size = size * nmemb;
	response_body->append(ptr, total_size);
	return total_size;
}

// Callback for libcurl to capture response headers.
struct ResponseHeaders {
	int32_t status_code = 0;
	std::string reason;
	case_insensitive_map_t<std::string> headers;
};

static size_t HeaderCallback(char *buffer, size_t size, size_t nitems, void *userdata) {
	auto *resp_headers = static_cast<ResponseHeaders *>(userdata);
	size_t total_size = size * nitems;
	std::string header_line(buffer, total_size);

	// Parse HTTP status line (e.g. "HTTP/1.1 200 OK\r\n").
	if (header_line.substr(0, 5) == "HTTP/") {
		auto space1 = header_line.find(' ');
		if (space1 != std::string::npos) {
			auto space2 = header_line.find(' ', space1 + 1);
			if (space2 != std::string::npos) {
				resp_headers->status_code = std::stoi(header_line.substr(space1 + 1, space2 - space1 - 1));
				auto end = header_line.find_first_of("\r\n", space2 + 1);
				resp_headers->reason = header_line.substr(space2 + 1, end - space2 - 1);
			}
		}
		return total_size;
	}

	// Parse regular header (e.g. "Content-Type: application/json\r\n").
	auto colon = header_line.find(':');
	if (colon != std::string::npos) {
		std::string key = header_line.substr(0, colon);
		std::string value = header_line.substr(colon + 1);
		// Trim leading/trailing whitespace and CRLF.
		auto start = value.find_first_not_of(" \t");
		auto end = value.find_last_not_of(" \t\r\n");
		if (start != std::string::npos && end != std::string::npos) {
			value = value.substr(start, end - start + 1);
		}
		resp_headers->headers[key] = value;
	}

	return total_size;
}

// Construct HTTP log message using DuckDB's Value::STRUCT format for native integration
// with DuckDB's structured logging (duckdb_logs table).
static std::string ConstructHTTPLogMessage(const std::string &method, const std::string &url,
                                           const case_insensitive_map_t<std::string> &request_headers,
                                           const std::string &request_body,
                                           std::chrono::system_clock::time_point start_time,
                                           std::chrono::system_clock::time_point end_time, int32_t status_code,
                                           const std::string &reason,
                                           const case_insensitive_map_t<std::string> &response_headers) {
	// Calculate duration in milliseconds.
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

	// Convert start_time to DuckDB timestamp.
	auto start_us = std::chrono::duration_cast<std::chrono::microseconds>(start_time.time_since_epoch()).count();
	auto start_timestamp = Timestamp::FromEpochMicroSeconds(start_us);

	// Build request headers as MAP value.
	vector<Value> req_header_keys;
	vector<Value> req_header_values;
	for (const auto &header : request_headers) {
		req_header_keys.emplace_back(header.first);
		req_header_values.emplace_back(header.second);
	}
	auto req_headers_value = Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, req_header_keys, req_header_values);

	// Build request struct.
	child_list_t<Value> request_child_list = {{"type", Value(method)},
	                                          {"url", Value(url)},
	                                          {"headers", req_headers_value},
	                                          {"start_time", Value::TIMESTAMP(start_timestamp)},
	                                          {"duration_ms", Value::BIGINT(duration_ms)}};
	auto request_value = Value::STRUCT(request_child_list);

	// Build response struct (or NULL if no response).
	Value response_value;
	if (status_code > 0) {
		vector<Value> resp_header_keys;
		vector<Value> resp_header_values;
		for (const auto &header : response_headers) {
			resp_header_keys.emplace_back(header.first);
			resp_header_values.emplace_back(header.second);
		}
		auto resp_headers_value =
		    Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, resp_header_keys, resp_header_values);

		child_list_t<Value> response_child_list = {
		    {"status", Value(std::to_string(status_code))}, {"reason", Value(reason)}, {"headers", resp_headers_value}};
		response_value = Value::STRUCT(response_child_list);
	}

	child_list_t<Value> child_list = {{"request", request_value}, {"response", response_value}};
	return Value::STRUCT(child_list).ToString();
}

ElasticsearchClient::ElasticsearchClient(const ElasticsearchConfig &config, shared_ptr<Logger> logger)
    : config_(config), logger_(std::move(logger)), curl_handle_(nullptr) {

	// Build the base URL once.
	std::string protocol = config_.use_ssl ? "https" : "http";
	base_url_ = protocol + "://" + config_.host + ":" + std::to_string(config_.port);

	// Initialize CURL handle for connection reuse.
	curl_handle_ = curl_easy_init();
	if (!curl_handle_) {
		throw IOException("Failed to initialize CURL handle");
	}

	ConfigureCurlHandle();
}

ElasticsearchClient::~ElasticsearchClient() {
	if (curl_handle_) {
		curl_easy_cleanup(curl_handle_);
		curl_handle_ = nullptr;
	}
}

void ElasticsearchClient::ConfigureCurlHandle() {
	// Configure timeouts (config_.timeout is in milliseconds).
	curl_easy_setopt(curl_handle_, CURLOPT_TIMEOUT_MS, static_cast<long>(config_.timeout));
	curl_easy_setopt(curl_handle_, CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(config_.timeout));

	// SSL verification.
	if (config_.use_ssl && !config_.verify_ssl) {
		curl_easy_setopt(curl_handle_, CURLOPT_SSL_VERIFYPEER, 0L);
		curl_easy_setopt(curl_handle_, CURLOPT_SSL_VERIFYHOST, 0L);
	} else {
		curl_easy_setopt(curl_handle_, CURLOPT_SSL_VERIFYPEER, 1L);
		curl_easy_setopt(curl_handle_, CURLOPT_SSL_VERIFYHOST, 2L);
	}

	// Follow redirects.
	curl_easy_setopt(curl_handle_, CURLOPT_FOLLOWLOCATION, 1L);

	// Basic auth.
	if (!config_.username.empty()) {
		std::string userpwd = config_.username + ":" + config_.password;
		curl_easy_setopt(curl_handle_, CURLOPT_USERPWD, userpwd.c_str());
	}

	// Proxy configuration (from DuckDB's core HTTP proxy settings).
	if (!config_.proxy_host.empty()) {
		curl_easy_setopt(curl_handle_, CURLOPT_PROXY, config_.proxy_host.c_str());

		if (!config_.proxy_username.empty()) {
			std::string proxy_userpwd = config_.proxy_username + ":" + config_.proxy_password;
			curl_easy_setopt(curl_handle_, CURLOPT_PROXYUSERPWD, proxy_userpwd.c_str());
		}
	}

	// Set callbacks.
	curl_easy_setopt(curl_handle_, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl_handle_, CURLOPT_HEADERFUNCTION, HeaderCallback);

	// Enable TCP keep-alive for connection reuse.
	curl_easy_setopt(curl_handle_, CURLOPT_TCP_KEEPALIVE, 1L);
}

ElasticsearchResponse ElasticsearchClient::PerformRequest(const std::string &method, const std::string &path,
                                                          const std::string &body) {
	ElasticsearchResponse response;
	response.success = false;
	response.status_code = 0;

	// Record start time for logging.
	auto start_time = std::chrono::system_clock::now();
	bool should_log = logger_ && logger_->ShouldLog(HTTPLogType::NAME, HTTPLogType::LEVEL);

	// Track request headers for logging.
	case_insensitive_map_t<std::string> request_headers;
	request_headers["Accept"] = "application/json";

	try {
		std::string url = base_url_ + path;
		std::string response_body;
		ResponseHeaders resp_headers;

		// Reset handle state for this request (keeps connection alive).
		curl_easy_setopt(curl_handle_, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl_handle_, CURLOPT_WRITEDATA, &response_body);
		curl_easy_setopt(curl_handle_, CURLOPT_HEADERDATA, &resp_headers);

		// Build request headers.
		struct curl_slist *headers = nullptr;
		headers = curl_slist_append(headers, "Accept: application/json");

		// Configure method and body.
		if (method == "GET") {
			curl_easy_setopt(curl_handle_, CURLOPT_HTTPGET, 1L);
			curl_easy_setopt(curl_handle_, CURLOPT_CUSTOMREQUEST, nullptr);
		} else if (method == "POST") {
			curl_easy_setopt(curl_handle_, CURLOPT_POST, 1L);
			curl_easy_setopt(curl_handle_, CURLOPT_CUSTOMREQUEST, nullptr);
			curl_easy_setopt(curl_handle_, CURLOPT_POSTFIELDS, body.c_str());
			curl_easy_setopt(curl_handle_, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
			headers = curl_slist_append(headers, "Content-Type: application/json");
			request_headers["Content-Type"] = "application/json";
		} else if (method == "PUT") {
			curl_easy_setopt(curl_handle_, CURLOPT_CUSTOMREQUEST, "PUT");
			curl_easy_setopt(curl_handle_, CURLOPT_POSTFIELDS, body.c_str());
			curl_easy_setopt(curl_handle_, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
			headers = curl_slist_append(headers, "Content-Type: application/json");
			request_headers["Content-Type"] = "application/json";
		} else if (method == "DELETE") {
			curl_easy_setopt(curl_handle_, CURLOPT_CUSTOMREQUEST, "DELETE");
			if (!body.empty()) {
				curl_easy_setopt(curl_handle_, CURLOPT_POSTFIELDS, body.c_str());
				curl_easy_setopt(curl_handle_, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
				headers = curl_slist_append(headers, "Content-Type: application/json");
				request_headers["Content-Type"] = "application/json";
			}
		} else {
			response.error_message = "Unsupported HTTP method: " + method;
			return response;
		}

		curl_easy_setopt(curl_handle_, CURLOPT_HTTPHEADER, headers);

		// Perform the request.
		CURLcode res = curl_easy_perform(curl_handle_);

		// Clean up headers list.
		curl_slist_free_all(headers);

		if (res == CURLE_OK) {
			long http_code = 0;
			curl_easy_getinfo(curl_handle_, CURLINFO_RESPONSE_CODE, &http_code);
			response.status_code = static_cast<int32_t>(http_code);
			response.body = std::move(response_body);
			response.success = (response.status_code >= 200 && response.status_code < 300);

			if (!response.success) {
				response.error_message = "HTTP " + std::to_string(response.status_code) + ": " + response.body;
			}
		} else {
			response.error_message = "HTTP " + method + " request failed: " + std::string(curl_easy_strerror(res));
		}

		// Log the request (works for both successful and failed requests, unlike httplib).
		if (should_log) {
			auto end_time = std::chrono::system_clock::now();
			std::string log_msg =
			    ConstructHTTPLogMessage(method, path, request_headers, body, start_time, end_time, response.status_code,
			                            resp_headers.reason, resp_headers.headers);
			logger_->WriteLog(HTTPLogType::NAME, HTTPLogType::LEVEL, log_msg);
		}

	} catch (const std::exception &e) {
		response.error_message = std::string("Exception during HTTP request: ") + e.what();

		// Log even on exception.
		if (should_log) {
			auto end_time = std::chrono::system_clock::now();
			case_insensitive_map_t<std::string> empty_headers;
			std::string log_msg = ConstructHTTPLogMessage(method, path, request_headers, body, start_time, end_time, 0,
			                                              "", empty_headers);
			logger_->WriteLog(HTTPLogType::NAME, HTTPLogType::LEVEL, log_msg);
		}
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
