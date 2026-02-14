#include "elasticsearch_common.hpp"
#include "elasticsearch_cache.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <algorithm>
#include <functional>
#include <iomanip>
#include <sstream>

namespace duckdb {

using namespace duckdb_yyjson;

// Formats a coordinate value as a string without trailing zeros.
// This produces cleaner GeoJSON output like [-74.006,40.7128] instead of [-74.006000,40.712800].
static std::string CoordinateToString(double val) {
	std::ostringstream oss;
	oss << std::setprecision(15) << val;
	std::string s = oss.str();

	// Remove trailing zeros after decimal point.
	if (s.find('.') != std::string::npos) {
		s.erase(s.find_last_not_of('0') + 1, std::string::npos);
		// Remove trailing decimal point if no decimals remain.
		if (s.back() == '.') {
			s.pop_back();
		}
	}
	return s;
}

std::string TrimString(const std::string &s) {
	size_t start = s.find_first_not_of(" \t\n\r");
	if (start == std::string::npos)
		return "";
	size_t end = s.find_last_not_of(" \t\n\r");
	return s.substr(start, end - start + 1);
}

// Helper to parse a coordinate pair "lon lat" from WKT.
static bool ParseWKTCoordinate(const std::string &s, double &lon, double &lat) {
	std::string trimmed = TrimString(s);
	size_t space_pos = trimmed.find(' ');
	if (space_pos == std::string::npos)
		return false;

	try {
		lon = std::stod(trimmed.substr(0, space_pos));
		lat = std::stod(TrimString(trimmed.substr(space_pos + 1)));
		return true;
	} catch (...) {
		return false;
	}
}

// Helper to parse a coordinate sequence "lon1 lat1, lon2 lat2, ..." into JSON array string.
static std::string ParseWKTCoordinateSequence(const std::string &s) {
	std::string result = "[";
	std::string remaining = s;
	bool first = true;

	while (!remaining.empty()) {
		size_t comma_pos = remaining.find(',');
		std::string coord_str;
		if (comma_pos == std::string::npos) {
			coord_str = remaining;
			remaining = "";
		} else {
			coord_str = remaining.substr(0, comma_pos);
			remaining = remaining.substr(comma_pos + 1);
		}

		double lon, lat;
		if (!ParseWKTCoordinate(coord_str, lon, lat)) {
			return "";
		}

		if (!first)
			result += ",";
		first = false;
		result += "[" + CoordinateToString(lon) + "," + CoordinateToString(lat) + "]";
	}

	result += "]";
	return result;
}

// Helper to find matching closing parenthesis.
static size_t FindMatchingParenthesis(const std::string &s, size_t open_pos) {
	int depth = 1;
	for (size_t i = open_pos + 1; i < s.size(); i++) {
		if (s[i] == '(')
			depth++;
		else if (s[i] == ')') {
			depth--;
			if (depth == 0)
				return i;
		}
	}
	return std::string::npos;
}

// Parse WKT POINT to GeoJSON.
static std::string WKTPointToGeoJSON(const std::string &wkt) {
	size_t paren_start = wkt.find('(');
	size_t paren_end = wkt.rfind(')');
	if (paren_start == std::string::npos || paren_end == std::string::npos || paren_end <= paren_start) {
		return "";
	}

	std::string coords = wkt.substr(paren_start + 1, paren_end - paren_start - 1);
	double lon, lat;
	if (!ParseWKTCoordinate(coords, lon, lat)) {
		return "";
	}

	return "{\"type\":\"Point\",\"coordinates\":[" + CoordinateToString(lon) + "," + CoordinateToString(lat) + "]}";
}

// Parse WKT LINESTRING to GeoJSON.
static std::string WKTLineStringToGeoJSON(const std::string &wkt) {
	size_t paren_start = wkt.find('(');
	size_t paren_end = wkt.rfind(')');
	if (paren_start == std::string::npos || paren_end == std::string::npos || paren_end <= paren_start) {
		return "";
	}

	std::string coords = wkt.substr(paren_start + 1, paren_end - paren_start - 1);
	std::string coord_array = ParseWKTCoordinateSequence(coords);
	if (coord_array.empty()) {
		return "";
	}

	return "{\"type\":\"LineString\",\"coordinates\":" + coord_array + "}";
}

// Parse WKT POLYGON to GeoJSON.
static std::string WKTPolygonToGeoJSON(const std::string &wkt) {
	size_t paren_start = wkt.find('(');
	if (paren_start == std::string::npos) {
		return "";
	}

	std::string rings_str = wkt.substr(paren_start + 1);
	// Remove trailing parenthesis.
	if (!rings_str.empty() && rings_str.back() == ')') {
		rings_str.pop_back();
	}

	std::string result = "{\"type\":\"Polygon\",\"coordinates\":[";
	bool first_ring = true;

	size_t pos = 0;
	while (pos < rings_str.size()) {
		// Find the next ring starting with parenthesis.
		size_t ring_start = rings_str.find('(', pos);
		if (ring_start == std::string::npos)
			break;

		size_t ring_end = FindMatchingParenthesis(rings_str, ring_start);
		if (ring_end == std::string::npos) {
			return "";
		}

		std::string ring_coords = rings_str.substr(ring_start + 1, ring_end - ring_start - 1);
		std::string coord_array = ParseWKTCoordinateSequence(ring_coords);
		if (coord_array.empty()) {
			return "";
		}

		if (!first_ring)
			result += ",";
		first_ring = false;
		result += coord_array;

		pos = ring_end + 1;
	}

	result += "]}";
	return result;
}

// Parse WKT MULTIPOINT to GeoJSON.
static std::string WKTMultiPointToGeoJSON(const std::string &wkt) {
	size_t paren_start = wkt.find('(');
	size_t paren_end = wkt.rfind(')');
	if (paren_start == std::string::npos || paren_end == std::string::npos || paren_end <= paren_start) {
		return "";
	}

	std::string content = wkt.substr(paren_start + 1, paren_end - paren_start - 1);
	content = TrimString(content);

	std::string result = "{\"type\":\"MultiPoint\",\"coordinates\":[";
	bool first = true;

	if (content.find('(') != std::string::npos) {
		// It's ((lon lat), (lon lat)) format.
		size_t pos = 0;
		while (pos < content.size()) {
			size_t point_start = content.find('(', pos);
			if (point_start == std::string::npos)
				break;

			size_t point_end = content.find(')', point_start);
			if (point_end == std::string::npos) {
				return "";
			}

			std::string point_coords = content.substr(point_start + 1, point_end - point_start - 1);
			double lon, lat;
			if (!ParseWKTCoordinate(point_coords, lon, lat)) {
				return "";
			}

			if (!first)
				result += ",";
			first = false;
			result += "[" + CoordinateToString(lon) + "," + CoordinateToString(lat) + "]";

			pos = point_end + 1;
		}
	} else {
		// It's (lon1 lat1, lon2 lat2) format (simple coordinate list).
		std::string coord_array = ParseWKTCoordinateSequence(content);
		if (coord_array.empty()) {
			return "";
		}
		// coord_array already has [], so strip them and use the content.
		result += coord_array.substr(1, coord_array.size() - 2);
		first = false;
	}

	result += "]}";
	return result;
}

// Parse WKT MULTILINESTRING to GeoJSON.
static std::string WKTMultiLineStringToGeoJSON(const std::string &wkt) {
	size_t paren_start = wkt.find('(');
	if (paren_start == std::string::npos) {
		return "";
	}

	std::string lines_str = wkt.substr(paren_start + 1);
	if (!lines_str.empty() && lines_str.back() == ')') {
		lines_str.pop_back();
	}

	std::string result = "{\"type\":\"MultiLineString\",\"coordinates\":[";
	bool first_line = true;

	size_t pos = 0;
	while (pos < lines_str.size()) {
		size_t line_start = lines_str.find('(', pos);
		if (line_start == std::string::npos)
			break;

		size_t line_end = FindMatchingParenthesis(lines_str, line_start);
		if (line_end == std::string::npos) {
			return "";
		}

		std::string line_coords = lines_str.substr(line_start + 1, line_end - line_start - 1);
		std::string coord_array = ParseWKTCoordinateSequence(line_coords);
		if (coord_array.empty()) {
			return "";
		}

		if (!first_line)
			result += ",";
		first_line = false;
		result += coord_array;

		pos = line_end + 1;
	}

	result += "]}";
	return result;
}

// Parse WKT MULTIPOLYGON to GeoJSON.
static std::string WKTMultiPolygonToGeoJSON(const std::string &wkt) {
	size_t paren_start = wkt.find('(');
	if (paren_start == std::string::npos) {
		return "";
	}

	std::string polys_str = wkt.substr(paren_start + 1);
	if (!polys_str.empty() && polys_str.back() == ')') {
		polys_str.pop_back();
	}

	std::string result = "{\"type\":\"MultiPolygon\",\"coordinates\":[";
	bool first_poly = true;

	size_t pos = 0;
	while (pos < polys_str.size()) {
		// Find the start of a polygon (double parenthesis).
		size_t poly_start = polys_str.find('(', pos);
		if (poly_start == std::string::npos)
			break;

		size_t poly_end = FindMatchingParenthesis(polys_str, poly_start);
		if (poly_end == std::string::npos) {
			return "";
		}

		// Parse the rings within this polygon.
		std::string rings_str = polys_str.substr(poly_start + 1, poly_end - poly_start - 1);

		std::string poly_coords = "[";
		bool first_ring = true;

		size_t ring_pos = 0;
		while (ring_pos < rings_str.size()) {
			size_t ring_start = rings_str.find('(', ring_pos);
			if (ring_start == std::string::npos)
				break;

			size_t ring_end = FindMatchingParenthesis(rings_str, ring_start);
			if (ring_end == std::string::npos) {
				return "";
			}

			std::string ring_coords = rings_str.substr(ring_start + 1, ring_end - ring_start - 1);
			std::string coord_array = ParseWKTCoordinateSequence(ring_coords);
			if (coord_array.empty()) {
				return "";
			}

			if (!first_ring)
				poly_coords += ",";
			first_ring = false;
			poly_coords += coord_array;

			ring_pos = ring_end + 1;
		}

		poly_coords += "]";

		if (!first_poly)
			result += ",";
		first_poly = false;
		result += poly_coords;

		pos = poly_end + 1;
	}

	result += "]}";
	return result;
}

// Parse WKT GEOMETRYCOLLECTION to GeoJSON.
static std::string WKTGeometryCollectionToGeoJSON(const std::string &wkt) {
	size_t paren_start = wkt.find('(');
	size_t paren_end = wkt.rfind(')');
	if (paren_start == std::string::npos || paren_end == std::string::npos || paren_end <= paren_start) {
		return "";
	}

	std::string content = wkt.substr(paren_start + 1, paren_end - paren_start - 1);

	std::string result = "{\"type\":\"GeometryCollection\",\"geometries\":[";
	bool first = true;

	// Parse individual geometries (separated by commas at depth 0).
	size_t pos = 0;
	while (pos < content.size()) {
		// Skip whitespace and commas.
		while (pos < content.size() && (content[pos] == ' ' || content[pos] == ',' || content[pos] == '\t' ||
		                                content[pos] == '\n' || content[pos] == '\r')) {
			pos++;
		}
		if (pos >= content.size())
			break;

		// Find the geometry type keyword.
		size_t geom_start = pos;

		// Find the opening parenthesis of this geometry.
		size_t geom_paren = content.find('(', pos);
		if (geom_paren == std::string::npos)
			break;

		// Find the matching closing parenthesis.
		size_t geom_end = FindMatchingParenthesis(content, geom_paren);
		if (geom_end == std::string::npos) {
			return "";
		}

		std::string sub_wkt = content.substr(geom_start, geom_end - geom_start + 1);
		std::string sub_geojson = WKTToGeoJSON(sub_wkt);
		if (sub_geojson.empty()) {
			return "";
		}

		if (!first)
			result += ",";
		first = false;
		result += sub_geojson;

		pos = geom_end + 1;
	}

	result += "]}";
	return result;
}

// Convert WKT string to GeoJSON.
std::string WKTToGeoJSON(const std::string &wkt) {
	std::string trimmed = TrimString(wkt);
	if (trimmed.empty()) {
		return "";
	}

	// Check for WKT type keywords (uppercase only).
	if (trimmed.find("GEOMETRYCOLLECTION") == 0) {
		return WKTGeometryCollectionToGeoJSON(trimmed);
	} else if (trimmed.find("MULTIPOLYGON") == 0) {
		return WKTMultiPolygonToGeoJSON(trimmed);
	} else if (trimmed.find("MULTILINESTRING") == 0) {
		return WKTMultiLineStringToGeoJSON(trimmed);
	} else if (trimmed.find("MULTIPOINT") == 0) {
		return WKTMultiPointToGeoJSON(trimmed);
	} else if (trimmed.find("POLYGON") == 0) {
		return WKTPolygonToGeoJSON(trimmed);
	} else if (trimmed.find("LINESTRING") == 0) {
		return WKTLineStringToGeoJSON(trimmed);
	} else if (trimmed.find("POINT") == 0) {
		return WKTPointToGeoJSON(trimmed);
	}

	// Unknown WKT type.
	return "";
}

std::string GeoPointToGeoJSON(yyjson_val *val) {
	if (!val)
		return "";

	// geo_point can be in multiple formats:
	// 1. object: {"lat": 41.12, "lon": -71.34}
	// 2. array: [-71.34, 41.12] (lon, lat order)
	// 3. string: "41.12,-71.34" or geohash
	// 4. WKT: "POINT (-71.34 41.12)"

	if (yyjson_is_obj(val)) {
		yyjson_val *lat = yyjson_obj_get(val, "lat");
		yyjson_val *lon = yyjson_obj_get(val, "lon");
		if (lat && lon) {
			double lat_d = yyjson_is_real(lat) ? yyjson_get_real(lat) : static_cast<double>(yyjson_get_sint(lat));
			double lon_d = yyjson_is_real(lon) ? yyjson_get_real(lon) : static_cast<double>(yyjson_get_sint(lon));
			return "{\"type\":\"Point\",\"coordinates\":[" + CoordinateToString(lon_d) + "," +
			       CoordinateToString(lat_d) + "]}";
		}
	} else if (yyjson_is_arr(val)) {
		yyjson_val *lon = yyjson_arr_get(val, 0);
		yyjson_val *lat = yyjson_arr_get(val, 1);
		if (lat && lon) {
			double lat_d = yyjson_is_real(lat) ? yyjson_get_real(lat) : static_cast<double>(yyjson_get_sint(lat));
			double lon_d = yyjson_is_real(lon) ? yyjson_get_real(lon) : static_cast<double>(yyjson_get_sint(lon));
			return "{\"type\":\"Point\",\"coordinates\":[" + CoordinateToString(lon_d) + "," +
			       CoordinateToString(lat_d) + "]}";
		}
	} else if (yyjson_is_str(val)) {
		const char *str = yyjson_get_str(val);
		std::string s(str);

		// Check for WKT POINT format.
		if (s.find("POINT") == 0) {
			return WKTPointToGeoJSON(s);
		}

		// Check if it's "lat,lon" format.
		auto comma_pos = s.find(',');
		if (comma_pos != std::string::npos) {
			try {
				double lat = std::stod(s.substr(0, comma_pos));
				double lon = std::stod(s.substr(comma_pos + 1));
				return "{\"type\":\"Point\",\"coordinates\":[" + CoordinateToString(lon) + "," +
				       CoordinateToString(lat) + "]}";
			} catch (...) {
				// Not a valid "lat,lon" string, return empty (might be geohash - not supported).
			}
		}
	}

	// Return empty string if we can't parse it (will result in NULL).
	return "";
}

std::string GeoShapeToGeoJSON(yyjson_val *val) {
	if (!val)
		return "";

	// geo_shape can be in GeoJSON format (object) or WKT format (string).
	if (yyjson_is_str(val)) {
		// WKT format, parse and convert to GeoJSON.
		const char *str = yyjson_get_str(val);
		std::string wkt(str);
		return WKTToGeoJSON(wkt);
	}

	// Object format (already GeoJSON), serialize it.
	char *json_str = yyjson_val_write(val, 0, nullptr);
	if (json_str) {
		std::string result(json_str);
		free(json_str);
		return result;
	}
	return "";
}

yyjson_val *GetValueByPath(yyjson_val *obj, const std::string &path) {
	if (!obj || !yyjson_is_obj(obj))
		return nullptr;

	size_t pos = 0;
	size_t dot_pos;
	yyjson_val *current = obj;
	std::string remaining = path;

	while ((dot_pos = remaining.find('.', pos)) != std::string::npos) {
		std::string key = remaining.substr(0, dot_pos);
		current = yyjson_obj_get(current, key.c_str());
		if (!current || !yyjson_is_obj(current))
			return nullptr;
		remaining = remaining.substr(dot_pos + 1);
	}

	return yyjson_obj_get(current, remaining.c_str());
}

LogicalType BuildStructTypeFromProperties(yyjson_val *properties) {
	if (!properties || !yyjson_is_obj(properties)) {
		return LogicalType::VARCHAR; // fallback to JSON string
	}

	child_list_t<LogicalType> struct_children;

	yyjson_obj_iter iter;
	yyjson_obj_iter_init(properties, &iter);
	yyjson_val *key;

	while ((key = yyjson_obj_iter_next(&iter))) {
		const char *field_name = yyjson_get_str(key);
		yyjson_val *field_def = yyjson_obj_iter_get_val(key);

		LogicalType child_type = BuildDuckDBTypeFromMapping(field_def);
		struct_children.push_back(make_pair(std::string(field_name), child_type));
	}

	if (struct_children.empty()) {
		return LogicalType::VARCHAR; // empty object -> JSON string
	}

	return LogicalType::STRUCT(struct_children);
}

LogicalType BuildDuckDBTypeFromMapping(yyjson_val *field_def) {
	if (!field_def) {
		return LogicalType::VARCHAR;
	}

	yyjson_val *type_val = yyjson_obj_get(field_def, "type");
	yyjson_val *properties = yyjson_obj_get(field_def, "properties");

	// If it has properties (object or nested type).
	if (properties && yyjson_is_obj(properties)) {
		// Build STRUCT type from properties.
		return BuildStructTypeFromProperties(properties);
	}

	// Handle explicit type.
	if (type_val) {
		const char *type_str = yyjson_get_str(type_val);
		if (type_str) {
			std::string es_type(type_str);

			if (es_type == "nested") {
				// Nested type (array of objects) always has properties.
				yyjson_val *nested_props = yyjson_obj_get(field_def, "properties");
				if (nested_props) {
					LogicalType element_type = BuildStructTypeFromProperties(nested_props);
					return LogicalType::LIST(element_type);
				}
				return LogicalType::LIST(LogicalType::VARCHAR);
			} else if (es_type == "object") {
				yyjson_val *obj_props = yyjson_obj_get(field_def, "properties");
				if (obj_props) {
					return BuildStructTypeFromProperties(obj_props);
				}
				return LogicalType::VARCHAR; // object without properties -> JSON
			} else if (es_type == "text" || es_type == "keyword" || es_type == "string") {
				return LogicalType::VARCHAR;
			} else if (es_type == "long") {
				return LogicalType::BIGINT;
			} else if (es_type == "integer") {
				return LogicalType::INTEGER;
			} else if (es_type == "short") {
				return LogicalType::SMALLINT;
			} else if (es_type == "byte") {
				return LogicalType::TINYINT;
			} else if (es_type == "double") {
				return LogicalType::DOUBLE;
			} else if (es_type == "float" || es_type == "half_float") {
				return LogicalType::FLOAT;
			} else if (es_type == "boolean") {
				return LogicalType::BOOLEAN;
			} else if (es_type == "date") {
				return LogicalType::TIMESTAMP;
			} else if (es_type == "ip") {
				return LogicalType::VARCHAR;
			} else if (es_type == "geo_point" || es_type == "geo_shape") {
				// Return as VARCHAR containing GeoJSON. User can use ST_GeomFromGeoJSON if spatial
				// extension is loaded.
				return LogicalType::VARCHAR;
			} else if (es_type == "nested" || es_type == "object") {
				return LogicalType::VARCHAR; // return nested objects as JSON string (fallback)
			}
			return LogicalType::VARCHAR; // default to VARCHAR
		}
	}

	return LogicalType::VARCHAR;
}

void ParseMapping(yyjson_val *properties, const std::string &prefix, vector<string> &column_names,
                  vector<LogicalType> &column_types, vector<string> &field_paths, vector<string> &es_types) {
	if (!properties || !yyjson_is_obj(properties))
		return;

	yyjson_obj_iter iter;
	yyjson_obj_iter_init(properties, &iter);
	yyjson_val *key;

	while ((key = yyjson_obj_iter_next(&iter))) {
		const char *field_name = yyjson_get_str(key);
		yyjson_val *field_def = yyjson_obj_iter_get_val(key);

		std::string full_path = prefix.empty() ? field_name : prefix + "." + field_name;

		yyjson_val *type_val = yyjson_obj_get(field_def, "type");
		yyjson_val *nested_props = yyjson_obj_get(field_def, "properties");

		// Use complex DuckDB types for nested objects.
		// Each top-level field becomes a column with nested objects as STRUCTs.
		LogicalType col_type = BuildDuckDBTypeFromMapping(field_def);

		// Determine the Elasticsearch type for special handling.
		std::string es_type_str;
		if (type_val) {
			const char *ts = yyjson_get_str(type_val);
			if (ts)
				es_type_str = ts;
		} else if (nested_props) {
			es_type_str = "object";
		}

		column_names.push_back(std::string(field_name));
		column_types.push_back(col_type);
		field_paths.push_back(full_path);
		es_types.push_back(es_type_str);
	}
}

void CollectAllMappedPaths(yyjson_val *properties, const std::string &prefix, std::set<std::string> &paths) {
	if (!properties || !yyjson_is_obj(properties))
		return;

	yyjson_obj_iter iter;
	yyjson_obj_iter_init(properties, &iter);
	yyjson_val *key;

	while ((key = yyjson_obj_iter_next(&iter))) {
		const char *field_name = yyjson_get_str(key);
		yyjson_val *field_def = yyjson_obj_iter_get_val(key);

		std::string full_path = prefix.empty() ? field_name : prefix + "." + field_name;
		paths.insert(full_path);

		// Recursively collect nested paths for object/nested types.
		yyjson_val *nested_props = yyjson_obj_get(field_def, "properties");
		if (nested_props && yyjson_is_obj(nested_props)) {
			CollectAllMappedPaths(nested_props, full_path, paths);
		}
	}
}

void CollectAllPathTypes(yyjson_val *properties, const std::string &prefix,
                         std::unordered_map<std::string, std::string> &path_types) {
	if (!properties || !yyjson_is_obj(properties))
		return;

	yyjson_obj_iter iter;
	yyjson_obj_iter_init(properties, &iter);
	yyjson_val *key;

	while ((key = yyjson_obj_iter_next(&iter))) {
		const char *field_name = yyjson_get_str(key);
		yyjson_val *field_def = yyjson_obj_iter_get_val(key);

		std::string full_path = prefix.empty() ? field_name : prefix + "." + field_name;

		// Get Elasticsearch type for this field.
		yyjson_val *type_val = yyjson_obj_get(field_def, "type");
		if (type_val && yyjson_is_str(type_val)) {
			path_types[full_path] = yyjson_get_str(type_val);
		}

		// Recursively collect nested paths for object/nested types.
		yyjson_val *nested_props = yyjson_obj_get(field_def, "properties");
		if (nested_props && yyjson_is_obj(nested_props)) {
			CollectAllPathTypes(nested_props, full_path, path_types);
		}
	}
}

void CollectTextFieldsWithKeyword(yyjson_val *properties, const std::string &prefix,
                                  std::unordered_set<std::string> &text_fields_with_keyword) {
	if (!properties || !yyjson_is_obj(properties))
		return;

	yyjson_obj_iter iter;
	yyjson_obj_iter_init(properties, &iter);
	yyjson_val *key;

	while ((key = yyjson_obj_iter_next(&iter))) {
		const char *field_name = yyjson_get_str(key);
		yyjson_val *field_def = yyjson_obj_iter_get_val(key);

		std::string full_path = prefix.empty() ? field_name : prefix + "." + field_name;

		// Check if this is a text field with a .keyword subfield.
		yyjson_val *type_val = yyjson_obj_get(field_def, "type");
		if (type_val && yyjson_is_str(type_val)) {
			const char *type_str = yyjson_get_str(type_val);
			if (type_str && std::string(type_str) == "text") {
				// Check for "fields" property containing a "keyword" subfield.
				yyjson_val *fields = yyjson_obj_get(field_def, "fields");
				if (fields && yyjson_is_obj(fields)) {
					yyjson_val *keyword_field = yyjson_obj_get(fields, "keyword");
					if (keyword_field && yyjson_is_obj(keyword_field)) {
						// Verify it's actually a keyword type.
						yyjson_val *keyword_type = yyjson_obj_get(keyword_field, "type");
						if (keyword_type && yyjson_is_str(keyword_type)) {
							const char *kt = yyjson_get_str(keyword_type);
							if (kt && std::string(kt) == "keyword") {
								text_fields_with_keyword.insert(full_path);
							}
						}
					}
				}
			}
		}

		// Recursively collect nested paths for object/nested types.
		yyjson_val *nested_props = yyjson_obj_get(field_def, "properties");
		if (nested_props && yyjson_is_obj(nested_props)) {
			CollectTextFieldsWithKeyword(nested_props, full_path, text_fields_with_keyword);
		}
	}
}

bool AreTypesCompatible(const LogicalType &type1, const LogicalType &type2) {
	// Identical types are always compatible.
	if (type1 == type2) {
		return true;
	}

	// Both must be same type id for compatibility.
	if (type1.id() != type2.id()) {
		return false;
	}

	// For STRUCT types, check that all children are compatible.
	if (type1.id() == LogicalTypeId::STRUCT) {
		auto &children1 = StructType::GetChildTypes(type1);
		auto &children2 = StructType::GetChildTypes(type2);

		// Build maps for easier lookup.
		std::map<std::string, LogicalType> map1, map2;
		for (const auto &child : children1) {
			map1[child.first] = child.second;
		}
		for (const auto &child : children2) {
			map2[child.first] = child.second;
		}

		// Check overlapping fields have compatible types.
		for (const auto &kv : map1) {
			auto it = map2.find(kv.first);
			if (it != map2.end()) {
				if (!AreTypesCompatible(kv.second, it->second)) {
					return false;
				}
			}
		}
		return true;
	}

	// For LIST types, check child types.
	if (type1.id() == LogicalTypeId::LIST) {
		return AreTypesCompatible(ListType::GetChildType(type1), ListType::GetChildType(type2));
	}

	return false;
}

LogicalType MergeStructTypes(const LogicalType &type1, const LogicalType &type2) {
	if (type1.id() != LogicalTypeId::STRUCT || type2.id() != LogicalTypeId::STRUCT) {
		// If not both structs, prefer first.
		return type1;
	}

	auto &children1 = StructType::GetChildTypes(type1);
	auto &children2 = StructType::GetChildTypes(type2);

	// Build ordered map to preserve field order while merging.
	std::map<std::string, LogicalType> merged;
	std::vector<std::string> field_order;

	for (const auto &child : children1) {
		merged[child.first] = child.second;
		field_order.push_back(child.first);
	}

	for (const auto &child : children2) {
		auto it = merged.find(child.first);
		if (it != merged.end()) {
			// Field exists in both, merge recursively if both are structs.
			if (it->second.id() == LogicalTypeId::STRUCT && child.second.id() == LogicalTypeId::STRUCT) {
				merged[child.first] = MergeStructTypes(it->second, child.second);
			}
			// Otherwise keep the first type (already validated as compatible).
		} else {
			merged[child.first] = child.second;
			field_order.push_back(child.first);
		}
	}

	child_list_t<LogicalType> result;
	for (const auto &name : field_order) {
		result.push_back(make_pair(name, merged[name]));
	}

	return LogicalType::STRUCT(result);
}

void MergeMappingsFromIndices(yyjson_val *root, vector<string> &column_names, vector<LogicalType> &column_types,
                              vector<string> &field_paths, vector<string> &es_types,
                              std::set<std::string> &all_mapped_paths) {
	// Map from field path to merged info.
	std::map<std::string, MergedFieldInfo> merged_fields;
	std::vector<std::string> field_order; // preserve insertion order

	// Iterate over all indices in the response.
	yyjson_obj_iter root_iter;
	yyjson_obj_iter_init(root, &root_iter);
	yyjson_val *index_key;

	while ((index_key = yyjson_obj_iter_next(&root_iter))) {
		const char *index_name = yyjson_get_str(index_key);
		yyjson_val *index_obj = yyjson_obj_iter_get_val(index_key);
		if (!index_obj)
			continue;

		yyjson_val *mappings = yyjson_obj_get(index_obj, "mappings");
		if (!mappings)
			continue;

		yyjson_val *properties = yyjson_obj_get(mappings, "properties");
		if (!properties || !yyjson_is_obj(properties))
			continue;

		// Collect all mapped paths including nested for unmapped field detection.
		CollectAllMappedPaths(properties, "", all_mapped_paths);

		// Parse this index's mapping into temporary vectors.
		vector<string> idx_names, idx_paths, idx_es_types;
		vector<LogicalType> idx_types;
		ParseMapping(properties, "", idx_names, idx_types, idx_paths, idx_es_types);

		// Merge each field.
		for (size_t i = 0; i < idx_names.size(); i++) {
			const std::string &field_path = idx_paths[i];
			auto it = merged_fields.find(field_path);

			if (it == merged_fields.end()) {
				// New field, add it.
				MergedFieldInfo info;
				info.type = idx_types[i];
				info.es_type = idx_es_types[i];
				info.first_index = index_name;
				merged_fields[field_path] = info;
				field_order.push_back(field_path);
			} else {
				// Existing field, check compatibility.
				if (!AreTypesCompatible(it->second.type, idx_types[i])) {
					throw InvalidInputException(
					    "Incompatible field types for '%s': index '%s' has type %s, but index '%s' has type %s",
					    field_path, it->second.first_index, it->second.type.ToString(), index_name,
					    idx_types[i].ToString());
				}

				// Merge struct types to include all fields from both.
				if (it->second.type.id() == LogicalTypeId::STRUCT && idx_types[i].id() == LogicalTypeId::STRUCT) {
					it->second.type = MergeStructTypes(it->second.type, idx_types[i]);
				}
			}
		}
	}

	// Build output vectors in order.
	for (const auto &field_path : field_order) {
		const auto &info = merged_fields[field_path];

		// Extract column name from field path (last component).
		std::string col_name = field_path;
		size_t dot_pos = field_path.rfind('.');
		if (dot_pos != std::string::npos) {
			col_name = field_path.substr(dot_pos + 1);
		}

		column_names.push_back(col_name);
		column_types.push_back(info.type);
		field_paths.push_back(field_path);
		es_types.push_back(info.es_type);
	}
}

SampleResult SampleDocuments(ElasticsearchClient &client, const std::string &index, const std::string &query,
                             const vector<string> &field_paths, const vector<string> &es_types,
                             const std::set<std::string> &all_mapped_paths, int64_t sample_size) {
	SampleResult result;
	result.has_unmapped_fields = false;

	if (sample_size <= 0 || field_paths.empty()) {
		return result;
	}

	// Build a set of field paths to skip (geo types use arrays for coordinates, not for multiple values).
	std::set<std::string> skip_fields;
	for (size_t i = 0; i < field_paths.size() && i < es_types.size(); i++) {
		if (es_types[i] == "geo_point" || es_types[i] == "geo_shape") {
			skip_fields.insert(field_paths[i]);
		}
	}

	// Helper lambda to check if a document has unmapped fields.
	auto check_for_unmapped = [&all_mapped_paths](yyjson_val *obj, const std::string &prefix) -> bool {
		std::function<bool(yyjson_val *, const std::string &)> check_recursive = [&](yyjson_val *o,
		                                                                             const std::string &p) -> bool {
			if (!o || !yyjson_is_obj(o))
				return false;

			yyjson_obj_iter iter;
			yyjson_obj_iter_init(o, &iter);
			yyjson_val *key;

			while ((key = yyjson_obj_iter_next(&iter))) {
				const char *field_name = yyjson_get_str(key);
				yyjson_val *field_val = yyjson_obj_iter_get_val(key);
				std::string field_path = p.empty() ? field_name : p + "." + field_name;

				// Check if this exact path is mapped.
				bool is_mapped = all_mapped_paths.count(field_path) > 0;

				if (!is_mapped) {
					// Check if any mapped path starts with this path (it's a parent of a mapped field).
					bool is_parent_of_mapped = false;
					for (const auto &mp : all_mapped_paths) {
						if (mp.find(field_path + ".") == 0) {
							is_parent_of_mapped = true;
							break;
						}
					}

					if (!is_parent_of_mapped) {
						// This is an unmapped field.
						return true;
					}
				}

				// If it's an object, recurse into it.
				if (yyjson_is_obj(field_val)) {
					if (check_recursive(field_val, field_path)) {
						return true;
					}
				}
			}
			return false;
		};

		return check_recursive(obj, prefix);
	};

	// Helper lambda to check if we have found everything we're looking for.
	auto all_detected = [&]() -> bool {
		bool all_arrays_found = (result.array_fields.size() + skip_fields.size() >= field_paths.size());
		return all_arrays_found && result.has_unmapped_fields;
	};

	// Helper lambda to process documents from a batch and check for arrays and unmapped fields.
	auto process_batch = [&](yyjson_val *hits_array, int64_t &docs_remaining) -> void {
		size_t idx, max;
		yyjson_val *hit;
		yyjson_arr_foreach(hits_array, idx, max, hit) {
			if (docs_remaining <= 0 || all_detected()) {
				break;
			}

			yyjson_val *source = yyjson_obj_get(hit, "_source");
			if (!source) {
				continue;
			}

			docs_remaining--;

			// Check for unmapped fields (only if not already detected).
			if (!result.has_unmapped_fields) {
				result.has_unmapped_fields = check_for_unmapped(source, "");
			}

			// Check each field path for arrays.
			for (const auto &field_path : field_paths) {
				// Skip geo fields (their array format represents coordinates, not multiple values).
				if (skip_fields.count(field_path)) {
					continue;
				}

				// Skip if already detected as array.
				if (result.array_fields.count(field_path)) {
					continue;
				}

				yyjson_val *val = GetValueByPath(source, field_path);
				if (val && yyjson_is_arr(val)) {
					result.array_fields.insert(field_path);
				}
			}
		}
	};

	// Use scroll API to fetch documents in batches until we have sampled enough or exhausted results.
	// The batch size is controlled by the URL parameter in ScrollSearch().
	int64_t docs_remaining = sample_size;
	std::string scroll_id;

	// Initial search request.
	auto response = client.ScrollSearch(index, query, "1m", sample_size);
	if (!response.success) {
		// If sampling fails, return empty result (conservative: no arrays/unmapped detected).
		return result;
	}

	// Process batches until we have sampled enough documents or exhausted results.
	while (docs_remaining > 0 && !all_detected()) {
		yyjson_doc *doc = yyjson_read(response.body.c_str(), response.body.size(), 0);
		if (!doc) {
			break;
		}

		yyjson_val *root = yyjson_doc_get_root(doc);
		yyjson_val *hits_obj = yyjson_obj_get(root, "hits");
		yyjson_val *hits_array = hits_obj ? yyjson_obj_get(hits_obj, "hits") : nullptr;

		// Extract scroll_id for cleanup and subsequent requests.
		yyjson_val *scroll_id_val = yyjson_obj_get(root, "_scroll_id");
		if (scroll_id_val && yyjson_is_str(scroll_id_val)) {
			scroll_id = yyjson_get_str(scroll_id_val);
		}

		if (!hits_array || !yyjson_is_arr(hits_array) || yyjson_arr_size(hits_array) == 0) {
			// No more documents to process.
			yyjson_doc_free(doc);
			break;
		}

		process_batch(hits_array, docs_remaining);
		yyjson_doc_free(doc);

		// Check if we need more documents.
		if (docs_remaining > 0 && !all_detected() && !scroll_id.empty()) {
			response = client.ScrollNext(scroll_id, "1m");
			if (!response.success) {
				break;
			}
		} else {
			break;
		}
	}

	// Clean up the scroll context.
	if (!scroll_id.empty()) {
		client.ClearScroll(scroll_id);
	}

	return result;
}

std::string CollectUnmappedFields(yyjson_val *source, const std::set<std::string> &mapped_paths,
                                  const std::string &prefix) {
	if (!source || !yyjson_is_obj(source)) {
		return "";
	}

	// Use yyjson mutable doc to build the unmapped object.
	yyjson_mut_doc *unmapped_doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *unmapped_root = yyjson_mut_obj(unmapped_doc);
	yyjson_mut_doc_set_root(unmapped_doc, unmapped_root);

	bool has_unmapped = false;

	// Recursive helper to collect unmapped fields.
	std::function<void(yyjson_val *, yyjson_mut_val *, const std::string &)> collect_unmapped =
	    [&](yyjson_val *obj, yyjson_mut_val *target, const std::string &current_prefix) {
		    if (!obj || !yyjson_is_obj(obj))
			    return;

		    yyjson_obj_iter iter;
		    yyjson_obj_iter_init(obj, &iter);
		    yyjson_val *key;

		    while ((key = yyjson_obj_iter_next(&iter))) {
			    const char *field_name = yyjson_get_str(key);
			    yyjson_val *field_val = yyjson_obj_iter_get_val(key);

			    std::string field_path = current_prefix.empty() ? field_name : current_prefix + "." + field_name;

			    // Check if this exact path is mapped.
			    bool is_mapped = mapped_paths.count(field_path) > 0;

			    // Also check if any mapped path starts with this path (it's a parent of a mapped field).
			    bool is_parent_of_mapped = false;
			    for (const auto &mapped_path : mapped_paths) {
				    if (mapped_path.find(field_path + ".") == 0) {
					    is_parent_of_mapped = true;
					    break;
				    }
			    }

			    if (is_mapped) {
				    // This field is mapped, but check if it is an object/nested type with child fields.
				    // If the field has no children in mapped_paths, it's a terminal type (geo_point, etc.)
				    // and we should NOT recurse into it even if the value is an object.
				    bool has_mapped_children = false;
				    for (const auto &mp : mapped_paths) {
					    if (mp.find(field_path + ".") == 0) {
						    has_mapped_children = true;
						    break;
					    }
				    }

				    if (has_mapped_children && yyjson_is_obj(field_val)) {
					    // This is an object/nested type with defined child fields, check for unmapped children.
					    yyjson_mut_val *sub_obj = yyjson_mut_obj(unmapped_doc);
					    bool sub_has_unmapped = false;

					    yyjson_obj_iter sub_iter;
					    yyjson_obj_iter_init(field_val, &sub_iter);
					    yyjson_val *sub_key;

					    while ((sub_key = yyjson_obj_iter_next(&sub_iter))) {
						    const char *subfield_name = yyjson_get_str(sub_key);
						    yyjson_val *subfield_val = yyjson_obj_iter_get_val(sub_key);
						    std::string subfield_path = field_path + "." + subfield_name;

						    if (mapped_paths.count(subfield_path) == 0) {
							    // Check if it's a parent of any mapped field.
							    bool is_sub_parent = false;
							    for (const auto &mp : mapped_paths) {
								    if (mp.find(subfield_path + ".") == 0) {
									    is_sub_parent = true;
									    break;
								    }
							    }

							    if (!is_sub_parent) {
								    // This subfield is unmapped, add it.
								    yyjson_mut_val *copied = yyjson_val_mut_copy(unmapped_doc, subfield_val);
								    yyjson_mut_obj_add_val(unmapped_doc, sub_obj, subfield_name, copied);
								    sub_has_unmapped = true;
							    } else {
								    // Recurse into this object.
								    yyjson_mut_val *nested_obj = yyjson_mut_obj(unmapped_doc);
								    collect_unmapped(subfield_val, nested_obj, subfield_path);
								    if (yyjson_mut_obj_size(nested_obj) > 0) {
									    yyjson_mut_obj_add_val(unmapped_doc, sub_obj, subfield_name, nested_obj);
									    sub_has_unmapped = true;
								    }
							    }
						    } else if (yyjson_is_obj(subfield_val)) {
							    // Recurse for nested mapped objects.
							    yyjson_mut_val *nested_obj = yyjson_mut_obj(unmapped_doc);
							    collect_unmapped(subfield_val, nested_obj, subfield_path);
							    if (yyjson_mut_obj_size(nested_obj) > 0) {
								    yyjson_mut_obj_add_val(unmapped_doc, sub_obj, subfield_name, nested_obj);
								    sub_has_unmapped = true;
							    }
						    }
					    }

					    if (sub_has_unmapped) {
						    yyjson_mut_obj_add_val(unmapped_doc, target, field_name, sub_obj);
						    has_unmapped = true;
					    }
				    }
				    // Terminal type (geo_point, keyword, etc.), do not recurse.
			    } else if (is_parent_of_mapped) {
				    // This is a parent object of mapped fields, recurse to find unmapped children.
				    if (yyjson_is_obj(field_val)) {
					    yyjson_mut_val *sub_obj = yyjson_mut_obj(unmapped_doc);
					    collect_unmapped(field_val, sub_obj, field_path);
					    if (yyjson_mut_obj_size(sub_obj) > 0) {
						    yyjson_mut_obj_add_val(unmapped_doc, target, field_name, sub_obj);
						    has_unmapped = true;
					    }
				    }
			    } else {
				    // This field is completely unmapped, add the entire value.
				    yyjson_mut_val *copied = yyjson_val_mut_copy(unmapped_doc, field_val);
				    yyjson_mut_obj_add_val(unmapped_doc, target, field_name, copied);
				    has_unmapped = true;
			    }
		    }
	    };

	collect_unmapped(source, unmapped_root, prefix);

	std::string result;
	if (has_unmapped) {
		char *json_str = yyjson_mut_write(unmapped_doc, 0, nullptr);
		if (json_str) {
			result = json_str;
			free(json_str);
		}
	}

	yyjson_mut_doc_free(unmapped_doc);
	return result;
}

void SetStructValueFromJSON(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type) {
	if (!val || yyjson_is_null(val)) {
		FlatVector::SetNull(result, row_idx, true);
		return;
	}

	if (!yyjson_is_obj(val)) {
		// If not an object, set null.
		FlatVector::SetNull(result, row_idx, true);
		return;
	}

	auto &child_entries = StructVector::GetEntries(result);
	auto &child_types = StructType::GetChildTypes(type);

	for (idx_t i = 0; i < child_entries.size(); i++) {
		const auto &child_name = child_types[i].first;
		const auto &child_type = child_types[i].second;

		yyjson_val *child_val = yyjson_obj_get(val, child_name.c_str());
		SetValueFromJSON(child_val, *child_entries[i], row_idx, child_type, "");
	}
}

void SetListValueFromJSON(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type,
                          const std::string &es_type) {
	auto list_data = FlatVector::GetData<list_entry_t>(result);

	if (!val || yyjson_is_null(val)) {
		FlatVector::SetNull(result, row_idx, true);
		return;
	}

	// Handle single value as single-element list (Elasticsearch can return single values for array fields).
	if (!yyjson_is_arr(val)) {
		// Single value, treat as list with one element.
		auto &child_vector = ListVector::GetEntry(result);
		idx_t current_size = ListVector::GetListSize(result);

		list_data[row_idx].offset = current_size;
		list_data[row_idx].length = 1;

		ListVector::SetListSize(result, current_size + 1);
		ListVector::Reserve(result, current_size + 1);

		auto &child_type = ListType::GetChildType(type);
		SetValueFromJSON(val, child_vector, current_size, child_type, es_type);
		return;
	}

	// Handle array.
	size_t arr_len = yyjson_arr_size(val);
	auto &child_vector = ListVector::GetEntry(result);
	idx_t current_size = ListVector::GetListSize(result);

	list_data[row_idx].offset = current_size;
	list_data[row_idx].length = arr_len;

	if (arr_len == 0) {
		return;
	}

	ListVector::SetListSize(result, current_size + arr_len);
	ListVector::Reserve(result, current_size + arr_len);

	auto &child_type = ListType::GetChildType(type);

	size_t idx, max;
	yyjson_val *elem;
	idx_t elem_idx = 0;
	yyjson_arr_foreach(val, idx, max, elem) {
		SetValueFromJSON(elem, child_vector, current_size + elem_idx, child_type, es_type);
		elem_idx++;
	}
}

void SetValueFromJSON(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type,
                      const std::string &es_type) {
	if (!val || yyjson_is_null(val)) {
		FlatVector::SetNull(result, row_idx, true);
		return;
	}

	// If the type is LIST, we need to handle it specially. Either the value is an array,
	// or it is a single value that should be wrapped in a single-element list.
	// This must be checked BEFORE any es_type-specific handling.
	if (type.id() == LogicalTypeId::LIST) {
		SetListValueFromJSON(val, result, row_idx, type, es_type);
		return;
	}

	// Handle geo_point and geo_shape specially, convert to GeoJSON string.
	if (es_type == "geo_point" || es_type == "geo_shape") {
		std::string geojson;
		if (es_type == "geo_point") {
			geojson = GeoPointToGeoJSON(val);
		} else {
			geojson = GeoShapeToGeoJSON(val);
		}

		if (geojson.empty()) {
			FlatVector::SetNull(result, row_idx, true);
		} else {
			auto str_val = StringVector::AddString(result, geojson);
			FlatVector::GetData<string_t>(result)[row_idx] = str_val;
		}
		return;
	}

	switch (type.id()) {
	case LogicalTypeId::VARCHAR: {
		if (yyjson_is_str(val)) {
			auto str_val = StringVector::AddString(result, yyjson_get_str(val));
			FlatVector::GetData<string_t>(result)[row_idx] = str_val;
		} else {
			// Convert non-string values to JSON string.
			char *json_str = yyjson_val_write(val, 0, nullptr);
			if (json_str) {
				auto str_val = StringVector::AddString(result, json_str);
				FlatVector::GetData<string_t>(result)[row_idx] = str_val;
				free(json_str);
			} else {
				FlatVector::SetNull(result, row_idx, true);
			}
		}
		break;
	}
	case LogicalTypeId::BIGINT:
		if (yyjson_is_int(val) || yyjson_is_sint(val)) {
			FlatVector::GetData<int64_t>(result)[row_idx] = yyjson_get_sint(val);
		} else if (yyjson_is_uint(val)) {
			FlatVector::GetData<int64_t>(result)[row_idx] = static_cast<int64_t>(yyjson_get_uint(val));
		} else {
			FlatVector::SetNull(result, row_idx, true);
		}
		break;
	case LogicalTypeId::INTEGER:
		if (yyjson_is_int(val) || yyjson_is_sint(val)) {
			FlatVector::GetData<int32_t>(result)[row_idx] = static_cast<int32_t>(yyjson_get_sint(val));
		} else {
			FlatVector::SetNull(result, row_idx, true);
		}
		break;
	case LogicalTypeId::SMALLINT:
		if (yyjson_is_int(val) || yyjson_is_sint(val)) {
			FlatVector::GetData<int16_t>(result)[row_idx] = static_cast<int16_t>(yyjson_get_sint(val));
		} else {
			FlatVector::SetNull(result, row_idx, true);
		}
		break;
	case LogicalTypeId::TINYINT:
		if (yyjson_is_int(val) || yyjson_is_sint(val)) {
			FlatVector::GetData<int8_t>(result)[row_idx] = static_cast<int8_t>(yyjson_get_sint(val));
		} else {
			FlatVector::SetNull(result, row_idx, true);
		}
		break;
	case LogicalTypeId::DOUBLE:
		if (yyjson_is_real(val)) {
			FlatVector::GetData<double>(result)[row_idx] = yyjson_get_real(val);
		} else if (yyjson_is_int(val)) {
			FlatVector::GetData<double>(result)[row_idx] = static_cast<double>(yyjson_get_sint(val));
		} else {
			FlatVector::SetNull(result, row_idx, true);
		}
		break;
	case LogicalTypeId::FLOAT:
		if (yyjson_is_real(val)) {
			FlatVector::GetData<float>(result)[row_idx] = static_cast<float>(yyjson_get_real(val));
		} else if (yyjson_is_int(val)) {
			FlatVector::GetData<float>(result)[row_idx] = static_cast<float>(yyjson_get_sint(val));
		} else {
			FlatVector::SetNull(result, row_idx, true);
		}
		break;
	case LogicalTypeId::BOOLEAN:
		if (yyjson_is_bool(val)) {
			FlatVector::GetData<bool>(result)[row_idx] = yyjson_get_bool(val);
		} else {
			FlatVector::SetNull(result, row_idx, true);
		}
		break;
	case LogicalTypeId::TIMESTAMP: {
		if (yyjson_is_str(val)) {
			// Try to parse ISO timestamp string.
			auto str = yyjson_get_str(val);
			timestamp_t ts;
			if (Timestamp::TryConvertTimestamp(str, strlen(str), ts, false, nullptr, false) ==
			    TimestampCastResult::SUCCESS) {
				FlatVector::GetData<timestamp_t>(result)[row_idx] = ts;
			} else {
				FlatVector::SetNull(result, row_idx, true);
			}
		} else if (yyjson_is_int(val)) {
			// Assume milliseconds since epoch.
			auto ms = yyjson_get_sint(val);
			FlatVector::GetData<timestamp_t>(result)[row_idx] = Timestamp::FromEpochMs(ms);
		} else {
			FlatVector::SetNull(result, row_idx, true);
		}
		break;
	}
	case LogicalTypeId::STRUCT:
		SetStructValueFromJSON(val, result, row_idx, type);
		break;
	default:
		FlatVector::SetNull(result, row_idx, true);
	}
}

std::string GetElasticsearchFieldName(const std::string &column_name, bool is_text_field, bool has_keyword_subfield) {
	// For text fields with .keyword subfield, use .keyword for exact matching.
	// Text fields are analyzed and do not support exact term queries on the base field.
	// For text fields without .keyword subfield, return the base field name.
	// The caller should decide whether to push down the filter or not.
	if (is_text_field && has_keyword_subfield) {
		return column_name + ".keyword";
	}
	return column_name;
}

yyjson_mut_val *DuckDBValueToJSON(yyjson_mut_doc *doc, const Value &value) {
	if (value.IsNull()) {
		return yyjson_mut_null(doc);
	}

	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return yyjson_mut_bool(doc, BooleanValue::Get(value));

	case LogicalTypeId::TINYINT:
		return yyjson_mut_sint(doc, TinyIntValue::Get(value));

	case LogicalTypeId::SMALLINT:
		return yyjson_mut_sint(doc, SmallIntValue::Get(value));

	case LogicalTypeId::INTEGER:
		return yyjson_mut_sint(doc, IntegerValue::Get(value));

	case LogicalTypeId::BIGINT:
		return yyjson_mut_sint(doc, BigIntValue::Get(value));

	case LogicalTypeId::UTINYINT:
		return yyjson_mut_uint(doc, UTinyIntValue::Get(value));

	case LogicalTypeId::USMALLINT:
		return yyjson_mut_uint(doc, USmallIntValue::Get(value));

	case LogicalTypeId::UINTEGER:
		return yyjson_mut_uint(doc, UIntegerValue::Get(value));

	case LogicalTypeId::UBIGINT:
		return yyjson_mut_uint(doc, UBigIntValue::Get(value));

	case LogicalTypeId::FLOAT:
		return yyjson_mut_real(doc, FloatValue::Get(value));

	case LogicalTypeId::DOUBLE:
		return yyjson_mut_real(doc, DoubleValue::Get(value));

	case LogicalTypeId::VARCHAR:
		return yyjson_mut_strcpy(doc, StringValue::Get(value).c_str());

	case LogicalTypeId::DATE: {
		// Convert to ISO 8601 date string (YYYY-MM-DD).
		// Date::ToString returns YYYY-MM-DD format which Elasticsearch accepts.
		auto date = DateValue::Get(value);
		auto str = Date::ToString(date);
		return yyjson_mut_strcpy(doc, str.c_str());
	}

	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS: {
		// Convert to ISO 8601 timestamp string.
		// Timestamp::ToString returns YYYY-MM-DD HH:MM:SS but Elasticsearch expects YYYY-MM-DDTHH:MM:SS.
		auto ts = TimestampValue::Get(value);
		auto str = Timestamp::ToString(ts);
		// Replace space with 'T' for ISO 8601 compliance.
		auto space_pos = str.find(' ');
		if (space_pos != string::npos) {
			str[space_pos] = 'T';
		}
		return yyjson_mut_strcpy(doc, str.c_str());
	}

	default:
		// For other types, convert to string.
		return yyjson_mut_strcpy(doc, value.ToString().c_str());
	}
}

bool ExtractConstantDouble(const Expression &expr, double &value) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}
	auto &const_expr = expr.Cast<BoundConstantExpression>();
	if (const_expr.value.IsNull()) {
		return false;
	}
	auto type_id = const_expr.value.type().id();
	if (type_id == LogicalTypeId::DOUBLE) {
		value = DoubleValue::Get(const_expr.value);
		return true;
	}
	if (type_id == LogicalTypeId::FLOAT) {
		value = static_cast<double>(FloatValue::Get(const_expr.value));
		return true;
	}
	if (type_id == LogicalTypeId::INTEGER) {
		value = static_cast<double>(IntegerValue::Get(const_expr.value));
		return true;
	}
	if (type_id == LogicalTypeId::BIGINT) {
		value = static_cast<double>(BigIntValue::Get(const_expr.value));
		return true;
	}
	if (type_id == LogicalTypeId::SMALLINT) {
		value = static_cast<double>(SmallIntValue::Get(const_expr.value));
		return true;
	}
	if (type_id == LogicalTypeId::TINYINT) {
		value = static_cast<double>(TinyIntValue::Get(const_expr.value));
		return true;
	}
	if (type_id == LogicalTypeId::HUGEINT) {
		value = Hugeint::Cast<double>(HugeIntValue::Get(const_expr.value));
		return true;
	}
	return false;
}

bool ExtractConstantString(const Expression &expr, std::string &value) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}
	auto &const_expr = expr.Cast<BoundConstantExpression>();
	if (const_expr.value.IsNull()) {
		return false;
	}
	if (const_expr.value.type().id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	value = StringValue::Get(const_expr.value);
	return true;
}

bool ExtractEnvelopeCoordinates(const Expression &expr, double &xmin, double &ymin, double &xmax, double &ymax) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}

	auto &func_expr = expr.Cast<BoundFunctionExpression>();
	if (StringUtil::Lower(func_expr.function.name) != "st_makeenvelope" || func_expr.children.size() != 4) {
		return false;
	}

	return ExtractConstantDouble(*func_expr.children[0], xmin) && ExtractConstantDouble(*func_expr.children[1], ymin) &&
	       ExtractConstantDouble(*func_expr.children[2], xmax) && ExtractConstantDouble(*func_expr.children[3], ymax);
}

bool ExtractPointCoordinates(const std::string &geojson, double &lon, double &lat) {
	yyjson_doc *doc = yyjson_read(geojson.c_str(), geojson.size(), 0);
	if (!doc) {
		return false;
	}

	yyjson_val *root = yyjson_doc_get_root(doc);
	yyjson_val *type_val = yyjson_obj_get(root, "type");
	if (!type_val || std::string(yyjson_get_str(type_val)) != "Point") {
		yyjson_doc_free(doc);
		return false;
	}

	yyjson_val *coords = yyjson_obj_get(root, "coordinates");
	if (!coords || !yyjson_is_arr(coords) || yyjson_arr_size(coords) < 2) {
		yyjson_doc_free(doc);
		return false;
	}

	yyjson_val *lon_val = yyjson_arr_get(coords, 0);
	yyjson_val *lat_val = yyjson_arr_get(coords, 1);
	if (!yyjson_is_num(lon_val) || !yyjson_is_num(lat_val)) {
		yyjson_doc_free(doc);
		return false;
	}

	lon = yyjson_get_real(lon_val);
	lat = yyjson_get_real(lat_val);

	// yyjson_get_real returns 0 for integers, need to check for int type.
	if (yyjson_is_int(lon_val)) {
		lon = static_cast<double>(yyjson_get_int(lon_val));
	}
	if (yyjson_is_int(lat_val)) {
		lat = static_cast<double>(yyjson_get_int(lat_val));
	}

	yyjson_doc_free(doc);
	return true;
}

bool IsGeoColumnRef(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &func_expr = expr.Cast<BoundFunctionExpression>();
	if (StringUtil::Lower(func_expr.function.name) != "st_geomfromgeojson" || func_expr.children.empty()) {
		return false;
	}
	// If the child is a column ref (or struct_extract chain), it's a geo field reference.
	// If it's a constant, it's a constant geometry.
	auto child_class = func_expr.children[0]->GetExpressionClass();
	if (child_class == ExpressionClass::BOUND_COLUMN_REF) {
		return true;
	}
	if (child_class == ExpressionClass::BOUND_FUNCTION) {
		auto &child_func = func_expr.children[0]->Cast<BoundFunctionExpression>();
		if (StringUtil::Lower(child_func.function.name) == "struct_extract") {
			return true;
		}
	}
	return false;
}

ElasticsearchSchemaResult ResolveElasticsearchSchema(const ElasticsearchConfig &config, const string &index,
                                                     const string &base_query, int64_t sample_size,
                                                     shared_ptr<Logger> logger) {
	// Check cache first.
	string cache_key = BuildBindCacheKey(config, index, base_query, sample_size);
	auto &cache = ElasticsearchBindCache::Instance();
	const auto *cached = cache.Get(cache_key);
	if (cached) {
		// Return a copy of the cached entry.
		ElasticsearchSchemaResult result;
		result.all_column_names = cached->all_column_names;
		result.all_column_types = cached->all_column_types;
		result.field_paths = cached->field_paths;
		result.es_types = cached->es_types;
		result.all_mapped_paths = cached->all_mapped_paths;
		result.es_type_map = cached->es_type_map;
		result.text_fields = cached->text_fields;
		result.text_fields_with_keyword = cached->text_fields_with_keyword;
		return result;
	}

	// Cache miss: fetch mapping and sample documents from Elasticsearch.
	ElasticsearchSchemaResult result;

	ElasticsearchClient client(config, logger);
	auto mapping_response = client.GetMapping(index);

	if (!mapping_response.success) {
		throw IOException("Failed to get Elasticsearch mapping: " + mapping_response.error_message);
	}

	// Parse the mapping response.
	yyjson_doc *doc = yyjson_read(mapping_response.body.c_str(), mapping_response.body.size(), 0);
	if (!doc) {
		throw IOException("Failed to parse Elasticsearch mapping response");
	}

	yyjson_val *root = yyjson_doc_get_root(doc);

	// Merge mappings from all matching indices.
	MergeMappingsFromIndices(root, result.all_column_names, result.all_column_types, result.field_paths,
	                         result.es_types, result.all_mapped_paths);

	// Collect all path types including nested paths (needed for filter pushdown on nested struct fields).
	// Also collect text fields that have a .keyword subfield (needed for filter pushdown on text fields).
	std::unordered_map<std::string, std::string> all_path_types;
	yyjson_obj_iter idx_iter;
	yyjson_obj_iter_init(root, &idx_iter);
	yyjson_val *idx_key;
	while ((idx_key = yyjson_obj_iter_next(&idx_iter))) {
		yyjson_val *idx_obj = yyjson_obj_iter_get_val(idx_key);
		yyjson_val *mappings = yyjson_obj_get(idx_obj, "mappings");
		if (mappings) {
			yyjson_val *properties = yyjson_obj_get(mappings, "properties");
			if (properties) {
				CollectAllPathTypes(properties, "", all_path_types);
				CollectTextFieldsWithKeyword(properties, "", result.text_fields_with_keyword);
			}
		}
	}

	yyjson_doc_free(doc);

	// Build Elasticsearch type map and identify text fields from top-level columns.
	for (size_t i = 0; i < result.all_column_names.size(); i++) {
		const string &col_name = result.all_column_names[i];
		const string &es_type = result.es_types[i];
		result.es_type_map[col_name] = es_type;
		if (es_type == "text") {
			result.text_fields.insert(col_name);
		}
	}

	// Add all nested paths to es_type_map and text_fields.
	for (const auto &entry : all_path_types) {
		result.es_type_map[entry.first] = entry.second;
		if (entry.second == "text") {
			result.text_fields.insert(entry.first);
		}
	}

	// Sample documents to detect arrays and unmapped fields.
	// Uses the user-provided query (base_query) if specified, otherwise match_all.
	// This is the best approximation of the actual query because filter pushdown (WHERE clauses)
	// happens after bind time, so the final query with pushed-down filters is not yet known.
	std::string sampling_query = R"({"query": {"match_all": {}}})";
	if (!base_query.empty()) {
		sampling_query = R"({"query": )" + base_query + "}";
	}
	if (sample_size > 0 && !result.field_paths.empty()) {
		SampleResult sample_result = SampleDocuments(client, index, sampling_query, result.field_paths, result.es_types,
		                                             result.all_mapped_paths, sample_size);

		// Wrap types in LIST for fields detected as arrays.
		for (size_t i = 0; i < result.field_paths.size(); i++) {
			if (sample_result.array_fields.count(result.field_paths[i])) {
				if (result.all_column_types[i].id() != LogicalTypeId::LIST) {
					result.all_column_types[i] = LogicalType::LIST(result.all_column_types[i]);
				}
			}
		}
	}

	// Store in cache for subsequent bind calls with the same parameters.
	ElasticsearchBindCacheEntry cache_entry;
	cache_entry.all_column_names = result.all_column_names;
	cache_entry.all_column_types = result.all_column_types;
	cache_entry.field_paths = result.field_paths;
	cache_entry.es_types = result.es_types;
	cache_entry.all_mapped_paths = result.all_mapped_paths;
	cache_entry.es_type_map = result.es_type_map;
	cache_entry.text_fields = result.text_fields;
	cache_entry.text_fields_with_keyword = result.text_fields_with_keyword;
	cache.Put(cache_key, std::move(cache_entry));

	return result;
}

} // namespace duckdb
