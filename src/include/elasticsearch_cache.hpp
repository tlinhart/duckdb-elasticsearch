#pragma once

#include "duckdb.hpp"
#include "elasticsearch_client.hpp"

#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

// Cached result of a bind operation (mapping + sampling).
// Contains all schema information derived from Elasticsearch that is needed by the bind function.
struct ElasticsearchBindCacheEntry {
	vector<string> all_column_names;
	vector<LogicalType> all_column_types; // final types (after array wrapping from sampling)
	vector<string> field_paths;
	vector<string> es_types;
	std::set<string> all_mapped_paths;
	std::unordered_map<string, string> es_type_map;
	std::unordered_set<string> text_fields;
	std::unordered_set<string> text_fields_with_keyword;
};

// Thread-safe per-process cache for Elasticsearch bind results.
// Prevents redundant mapping and sampling HTTP requests when DuckDB calls bind multiple times
// with the same parameters (e.g. UNPIVOT ... ON COLUMNS(*), CTEs referenced multiple times etc.)
class ElasticsearchBindCache {
public:
	static ElasticsearchBindCache &Instance();

	// Look up a cached entry by key. Returns nullptr if not found.
	// The returned pointer is invalidated by subsequent Put() or Clear() calls,
	// so callers should copy the data they need before releasing control.
	const ElasticsearchBindCacheEntry *Get(const string &key);

	// Store an entry in the cache, keyed by the given string.
	void Put(const string &key, ElasticsearchBindCacheEntry entry);

	// Clear all cached entries. Returns the number of entries that were cleared.
	idx_t Clear();

private:
	ElasticsearchBindCache() = default;

	std::mutex mutex_;
	std::unordered_map<string, ElasticsearchBindCacheEntry> cache_;
};

// Build a cache key from the resolved Elasticsearch configuration and query parameters.
// Includes parameters that affect the bind result: host, port, index, base query and sample size.
// Connection settings (credentials, SSL) and transport settings (timeout, retries) are excluded
// since they don't affect the schema.
string BuildBindCacheKey(const ElasticsearchConfig &config, const string &index, const string &base_query,
                         int64_t sample_size);

// Callback for extension settings that affect the bind cache (e.g. elasticsearch_sample_size).
// Clears the bind cache when the setting is changed so that stale schema results are not reused.
void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);

// Register the elasticsearch_clear_cache() scalar function.
void RegisterElasticsearchClearCacheFunction(ExtensionLoader &loader);

} // namespace duckdb
