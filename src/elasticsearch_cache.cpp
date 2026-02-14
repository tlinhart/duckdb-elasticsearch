#include "elasticsearch_cache.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

ElasticsearchBindCache &ElasticsearchBindCache::Instance() {
	static ElasticsearchBindCache instance;
	return instance;
}

const ElasticsearchBindCacheEntry *ElasticsearchBindCache::Get(const string &key) {
	lock_guard<mutex> lock(mutex_);
	auto it = cache_.find(key);
	if (it != cache_.end()) {
		return &it->second;
	}
	return nullptr;
}

void ElasticsearchBindCache::Put(const string &key, ElasticsearchBindCacheEntry entry) {
	lock_guard<mutex> lock(mutex_);
	cache_[key] = std::move(entry);
}

idx_t ElasticsearchBindCache::Clear() {
	lock_guard<mutex> lock(mutex_);
	idx_t count = cache_.size();
	cache_.clear();
	return count;
}

string BuildBindCacheKey(const ElasticsearchConfig &config, const string &index, const string &base_query,
                         int64_t sample_size) {
	// Use null byte as separator since it won't appear in normal parameter values.
	string key;
	key += config.host;
	key += '\0';
	key += to_string(config.port);
	key += '\0';
	key += index;
	key += '\0';
	key += base_query;
	key += '\0';
	key += config.username;
	key += '\0';
	key += config.password;
	key += '\0';
	key += config.use_ssl ? "1" : "0";
	key += '\0';
	key += config.verify_ssl ? "1" : "0";
	key += '\0';
	key += to_string(sample_size);
	return key;
}

// Scalar function that clears the per-process bind cache and returns true on success.
static void ElasticsearchClearCacheFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &cache = ElasticsearchBindCache::Instance();
	cache.Clear();
	result.SetValue(0, Value(true));
}

void RegisterElasticsearchClearCacheFunction(ExtensionLoader &loader) {
	// Mark as VOLATILE so DuckDB won't optimize away the call or cache its result.
	ScalarFunction clear_cache("elasticsearch_clear_cache", {}, LogicalType::BOOLEAN, ElasticsearchClearCacheFunction,
	                           nullptr, nullptr, nullptr, nullptr, LogicalType(LogicalTypeId::INVALID),
	                           FunctionStability::VOLATILE);
	loader.RegisterFunction(clear_cache);
}

} // namespace duckdb
