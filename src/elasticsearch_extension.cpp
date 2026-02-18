#define DUCKDB_EXTENSION_MAIN

#include "elasticsearch_extension.hpp"
#include "elasticsearch_cache.hpp"
#include "elasticsearch_query.hpp"
#include "elasticsearch_optimizer.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/config.hpp"

#include <curl/curl.h>

// Detect DuckDB v1.5+ by checking for the new extension_callback_manager header
// introduced in PR #20599. If __has_include is unsupported or the header is
// missing, we fall back to the old API (safe for older DuckDB versions).
#if defined(__has_include)
#if __has_include(<duckdb/main/extension_callback_manager.hpp>)
#define DUCKDB_V1_5_OR_LATER true
#endif
#endif

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Initialize libcurl globally. This must be called before any CURL handles are created.
	// It is safe to call multiple times (curl tracks init count internally).
	curl_global_init(CURL_GLOBAL_DEFAULT);

	// Register table functions.
	RegisterElasticsearchQueryFunction(loader);

	// Register scalar functions.
	RegisterElasticsearchClearCacheFunction(loader);

	// Register optimizer extension for LIMIT/OFFSET pushdown.
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
#ifdef DUCKDB_V1_5_OR_LATER
	OptimizerExtension::Register(config, ElasticsearchOptimizerExtension());
#else
	config.optimizer_extensions.push_back(ElasticsearchOptimizerExtension());
#endif

	// Register extension settings.
	// These provide configurable defaults for elasticsearch_query() parameters.
	// Named parameters on the function override these settings when specified.
	config.AddExtensionOption("elasticsearch_verify_ssl",
	                          "Whether to verify SSL certificates when connecting to Elasticsearch",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("elasticsearch_timeout", "Request timeout for Elasticsearch connections in milliseconds",
	                          LogicalType::INTEGER, Value::INTEGER(30000));
	config.AddExtensionOption("elasticsearch_max_retries",
	                          "Maximum number of retries for transient Elasticsearch errors", LogicalType::INTEGER,
	                          Value::INTEGER(3));
	config.AddExtensionOption("elasticsearch_retry_interval", "Initial wait time between retries in milliseconds",
	                          LogicalType::INTEGER, Value::INTEGER(100));
	config.AddExtensionOption("elasticsearch_retry_backoff_factor",
	                          "Exponential backoff factor applied between retries", LogicalType::DOUBLE,
	                          Value::DOUBLE(2.0));
	config.AddExtensionOption("elasticsearch_sample_size",
	                          "Number of documents to sample for array detection (0 to disable)", LogicalType::INTEGER,
	                          Value::INTEGER(100), ClearCacheOnSetting);
	config.AddExtensionOption("elasticsearch_batch_size",
	                          "Number of documents fetched per scroll batch from Elasticsearch", LogicalType::INTEGER,
	                          Value::INTEGER(1000));
	config.AddExtensionOption("elasticsearch_batch_size_threshold_factor",
	                          "For small LIMITs, fetch all rows in one request if total rows <= batch_size * factor",
	                          LogicalType::INTEGER, Value::INTEGER(5));
	config.AddExtensionOption("elasticsearch_scroll_time",
	                          "Scroll context keep-alive duration for data fetching (e.g. '5m', '1h')",
	                          LogicalType::VARCHAR, Value("5m"));
}

void ElasticsearchExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string ElasticsearchExtension::Name() {
	return "elasticsearch";
}

std::string ElasticsearchExtension::Version() const {
#ifdef EXT_VERSION_ELASTICSEARCH
	return EXT_VERSION_ELASTICSEARCH;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(elasticsearch, loader) {
	duckdb::LoadInternal(loader);
}
}
