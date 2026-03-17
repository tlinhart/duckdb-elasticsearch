#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/expression.hpp"
#include "yyjson.hpp"

#include <string>

namespace duckdb {

using namespace duckdb_yyjson;

// Extract value from a JSON object by dotted path (e.g. "address.city").
yyjson_val *GetValueByPath(yyjson_val *obj, const std::string &path);

// Convert a yyjson value to a DuckDB Vector entry at the given row index.
// Handles all DuckDB types including LIST, STRUCT and geo types (geo_point, geo_shape).
void ConvertJSONToDuckDB(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type,
                         const std::string &es_type);

// Convert a DuckDB Value to a yyjson mutable value for Elasticsearch query building.
// Handles all common DuckDB types including numeric, string, date and timestamp.
yyjson_mut_val *ConvertDuckDBToJSON(yyjson_mut_doc *doc, const Value &value);

// Convert WKT string to GeoJSON.
std::string WKTToGeoJSON(const std::string &wkt);

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
