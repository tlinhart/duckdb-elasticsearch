#pragma once

#include "duckdb.hpp"
#include "elasticsearch_client.hpp"

#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

// Unified schema information resolved from Elasticsearch mapping and document sampling.
// This is the single canonical representation of the schema for an Elasticsearch index (or index pattern).
// It is produced by ResolveElasticsearchSchema() and consumed by all downstream components:
// bind data, filter pushdown, query building and scanning.
struct ElasticsearchSchema {
	// Column names derived from the mapping (top-level field names).
	vector<string> column_names;

	// DuckDB types for each column. These are the final types after array wrapping from sampling,
	// i.e. fields detected as arrays during sampling have their type wrapped in LIST.
	vector<LogicalType> column_types;

	// Elasticsearch field paths (dotted notation for nested fields, e.g. "address.city").
	// Parallel to column_names/column_types - same index corresponds to the same field.
	vector<string> field_paths;

	// Elasticsearch type strings (e.g. "text", "keyword", "integer", "geo_point").
	// Parallel to column_names/column_types - same index corresponds to the same field.
	vector<string> es_types;

	// All mapped field paths including nested children (for unmapped field detection during scanning).
	std::set<string> all_mapped_paths;

	// Map from field name/path to Elasticsearch type string. Includes both top-level column names
	// and nested dotted paths (e.g. "address.city" -> "keyword"). Used for filter pushdown to
	// determine field types.
	std::unordered_map<string, string> es_type_map;

	// Set of field names/paths whose Elasticsearch type is "text" (analyzed, tokenized).
	// Text fields need .keyword subfield for exact matching in filters.
	std::unordered_set<string> text_fields;

	// Set of text field names/paths that have a .keyword subfield in the Elasticsearch mapping.
	// Only these text fields support filter pushdown (except IS NULL/IS NOT NULL which work on any field).
	std::unordered_set<string> text_fields_with_keyword;

	// Set of field names/paths whose Elasticsearch type is "geo_point" or "geo_shape".
	// Geo fields use spatial predicates (ST_Within, ST_DWithin, ST_Distance etc.) for pushdown;
	// standard comparison (=, !=, <, >, <=, >=) and IN operators cannot be pushed to Elasticsearch.
	std::unordered_set<string> geo_fields;
};

// Resolve schema for an Elasticsearch index, with caching.
// Fetches the index mapping and samples documents on cache miss; returns cached data on cache hit.
// Returns an ElasticsearchSchema containing all mapping and sampling information.
ElasticsearchSchema ResolveElasticsearchSchema(const ElasticsearchConfig &config, const string &index,
                                               const string &base_query, int64_t sample_size,
                                               shared_ptr<Logger> logger = nullptr);

// Callback for extension settings that affect the bind cache (e.g. elasticsearch_sample_size).
// Clears the cache when the setting is changed so that stale schema results are not reused.
void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);

// Register the elasticsearch_clear_cache() scalar function.
void RegisterElasticsearchClearCacheFunction(ExtensionLoader &loader);

} // namespace duckdb
