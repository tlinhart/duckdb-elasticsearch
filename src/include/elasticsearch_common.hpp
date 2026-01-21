#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "elasticsearch_client.hpp"
#include "yyjson.hpp"

#include <map>
#include <set>
#include <string>
#include <vector>

namespace duckdb {

using namespace duckdb_yyjson;

// Structure to hold merged mapping information for a field.
struct MergedFieldInfo {
	LogicalType type;
	std::string es_type;
	std::string first_index; // first index where this field was seen (for error messages)
};

// Build DuckDB type from Elasticsearch field definition.
LogicalType BuildDuckDBTypeFromMapping(yyjson_val *field_def);

// Build STRUCT type from Elasticsearch object/nested properties.
LogicalType BuildStructTypeFromProperties(yyjson_val *properties);

// Parse Elasticsearch mapping to extract field names and types.
void ParseMapping(yyjson_val *properties, const std::string &prefix, vector<string> &column_names,
                  vector<LogicalType> &column_types, vector<string> &field_paths, vector<string> &es_types);

// Recursively collect all field paths from Elasticsearch mapping properties (including nested children).
void CollectAllMappedPaths(yyjson_val *properties, const std::string &prefix, std::set<std::string> &paths);

// Merge mappings from multiple indices, checking for type compatibility.
void MergeMappingsFromIndices(yyjson_val *root, vector<string> &column_names, vector<LogicalType> &column_types,
                              vector<string> &field_paths, vector<string> &es_types,
                              std::set<std::string> &all_mapped_paths);

// Check if two DuckDB types are compatible for merging.
bool AreTypesCompatible(const LogicalType &type1, const LogicalType &type2);

// Merge two STRUCT types, combining all fields from both.
LogicalType MergeStructTypes(const LogicalType &type1, const LogicalType &type2);

// Result of sampling documents for schema inference.
struct SampleResult {
	std::set<std::string> array_fields; // fields detected as containing arrays
	bool has_unmapped_fields;           // whether any unmapped fields were found in the sample
};

// Sample documents to detect arrays and unmapped fields.
SampleResult SampleDocuments(ElasticsearchClient &client, const std::string &index, const std::string &query,
                             const vector<string> &field_paths, const vector<string> &es_types,
                             const std::set<std::string> &all_mapped_paths, int64_t sample_size);

// Helper function to extract value from a JSON object by path (supports nested fields with dots).
yyjson_val *GetValueByPath(yyjson_val *obj, const std::string &path);

// Extract value from yyjson_val and set it in the result vector.
void SetValueFromJson(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type,
                      const std::string &es_type);

// Set LIST value from JSON array.
void SetListValueFromJson(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type,
                          const std::string &es_type);

// Set STRUCT value from JSON object.
void SetStructValueFromJson(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type);

// Collect unmapped fields from _source that are not in the schema's field_paths.
std::string CollectUnmappedFields(yyjson_val *source, const std::set<std::string> &mapped_paths,
                                  const std::string &prefix = "");

// Convert Elasticsearch geo_point to GeoJSON format.
std::string GeoPointToGeoJSON(yyjson_val *val);

// Convert Elasticsearch geo_shape to GeoJSON (it's already GeoJSON-like or WKT string).
std::string GeoShapeToGeoJSON(yyjson_val *val);

// Convert WKT string to GeoJSON.
std::string WktToGeoJSON(const std::string &wkt);

// Helper to trim whitespace from string.
std::string TrimString(const std::string &s);

} // namespace duckdb
