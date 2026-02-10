#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression.hpp"
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

// Recursively collect all field paths with their Elasticsearch types from mapping properties.
// This includes nested paths for object/nested types.
void CollectAllPathTypes(yyjson_val *properties, const std::string &prefix,
                         std::unordered_map<std::string, std::string> &path_types);

// Collect text fields that have a .keyword subfield.
// These text fields can be filtered via the .keyword subfield which stores the raw
// (not analyzed) value. Text fields without .keyword cannot be filtered (except IS NULL/IS NOT NULL).
void CollectTextFieldsWithKeyword(yyjson_val *properties, const std::string &prefix,
                                  std::unordered_set<std::string> &text_fields_with_keyword);

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
void SetValueFromJSON(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type,
                      const std::string &es_type);

// Set LIST value from JSON array.
void SetListValueFromJSON(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type,
                          const std::string &es_type);

// Set STRUCT value from JSON object.
void SetStructValueFromJSON(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type);

// Collect unmapped fields from _source that are not in the schema's field_paths.
std::string CollectUnmappedFields(yyjson_val *source, const std::set<std::string> &mapped_paths,
                                  const std::string &prefix = "");

// Convert Elasticsearch geo_point to GeoJSON format.
std::string GeoPointToGeoJSON(yyjson_val *val);

// Convert Elasticsearch geo_shape to GeoJSON (it's already GeoJSON-like or WKT string).
std::string GeoShapeToGeoJSON(yyjson_val *val);

// Convert WKT string to GeoJSON.
std::string WKTToGeoJSON(const std::string &wkt);

// Helper to trim whitespace from string.
std::string TrimString(const std::string &s);

// Convert DuckDB value to yyjson mutable value for query building.
yyjson_mut_val *DuckDBValueToJSON(yyjson_mut_doc *doc, const Value &value);

// Get Elasticsearch field name, adding .keyword suffix for text fields that have a .keyword subfield.
// For text fields without .keyword subfield, returns the base field name (caller should handle appropriately).
std::string GetElasticsearchFieldName(const std::string &column_name, bool is_text_field, bool has_keyword_subfield);

// Extract constant double value from a BoundConstantExpression.
// Handles DOUBLE, FLOAT, INTEGER, BIGINT, SMALLINT, TINYINT and HUGEINT types.
bool ExtractConstantDouble(const Expression &expr, double &value);

// Extract constant string value from a BoundConstantExpression with VARCHAR type.
bool ExtractConstantString(const Expression &expr, std::string &value);

// Check if expression is ST_MakeEnvelope(xmin, ymin, xmax, ymax) and extract the coordinates.
bool ExtractEnvelopeCoordinates(const Expression &expr, double &xmin, double &ymin, double &xmax, double &ymax);

// Extract lat/lon from a GeoJSON Point string.
// Returns true if the GeoJSON is a Point and coordinates were extracted.
bool ExtractPointCoordinates(const std::string &geojson, double &lon, double &lat);

// Check if expression is ST_GeomFromGeoJSON(column_ref) i.e. references an Elasticsearch geo field.
// Detects the pattern ST_GeomFromGeoJSON(BOUND_COLUMN_REF) or ST_GeomFromGeoJSON(struct_extract(...)).
bool IsGeoColumnRef(const Expression &expr);

} // namespace duckdb
