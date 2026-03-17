#include "elasticsearch_common.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <cstring>

namespace duckdb {

using namespace duckdb_yyjson;

// Writes little-endian WKB binary into a growable buffer.
// The output can be stored directly in a GEOMETRY vector via StringVector::AddStringOrBlob().
class WKBWriter {
public:
	void WriteHeader(uint32_t type_id) {
		WriteByte(0x01); // little-endian byte order
		WriteUInt32(type_id);
	}

	void WritePoint(double x, double y) {
		WriteDouble(x);
		WriteDouble(y);
	}

	void WriteUInt32(uint32_t val) {
		buf.append(reinterpret_cast<const char *>(&val), sizeof(val));
	}

	void WriteDouble(double val) {
		buf.append(reinterpret_cast<const char *>(&val), sizeof(val));
	}

	void WriteByte(uint8_t val) {
		buf.push_back(static_cast<char>(val));
	}

	// Store the buffer into a GEOMETRY vector and return the string_t handle.
	string_t Store(Vector &result) {
		return StringVector::AddStringOrBlob(result, buf.data(), buf.size());
	}

private:
	std::string buf;
};

// WKB type IDs for 2D geometries.
static constexpr uint32_t WKB_POINT = 1;
static constexpr uint32_t WKB_LINESTRING = 2;
static constexpr uint32_t WKB_POLYGON = 3;
static constexpr uint32_t WKB_MULTIPOINT = 4;
static constexpr uint32_t WKB_MULTILINESTRING = 5;
static constexpr uint32_t WKB_MULTIPOLYGON = 6;
static constexpr uint32_t WKB_GEOMETRYCOLLECTION = 7;

// Forward declaration for recursive GeoJSON to WKB conversion.
static bool GeoJSONObjectToWKB(yyjson_val *val, WKBWriter &writer);

// Helper to get a numeric value from a yyjson value (handles both real and int).
static double YyjsonGetNum(yyjson_val *val) {
	if (yyjson_is_real(val)) {
		return yyjson_get_real(val);
	}
	if (yyjson_is_int(val)) {
		return static_cast<double>(yyjson_get_int(val));
	}
	if (yyjson_is_sint(val)) {
		return static_cast<double>(yyjson_get_sint(val));
	}
	return 0.0;
}

// Write a GeoJSON coordinates array [lon, lat] as a WKB point (no header).
static bool WriteGeoJSONCoordinates(yyjson_val *coord, WKBWriter &writer) {
	if (!coord || !yyjson_is_arr(coord) || yyjson_arr_size(coord) < 2) {
		return false;
	}
	double x = YyjsonGetNum(yyjson_arr_get(coord, 0));
	double y = YyjsonGetNum(yyjson_arr_get(coord, 1));
	writer.WritePoint(x, y);
	return true;
}

// Write a GeoJSON coordinate ring [[lon, lat], [lon, lat], ...] (writes count + points, no header).
static bool WriteGeoJSONCoordinateRing(yyjson_val *ring, WKBWriter &writer) {
	if (!ring || !yyjson_is_arr(ring)) {
		return false;
	}
	uint32_t count = static_cast<uint32_t>(yyjson_arr_size(ring));
	writer.WriteUInt32(count);
	size_t idx, max;
	yyjson_val *coord;
	yyjson_arr_foreach(ring, idx, max, coord) {
		if (!WriteGeoJSONCoordinates(coord, writer)) {
			return false;
		}
	}
	return true;
}

// Recursively convert a GeoJSON object to WKB.
static bool GeoJSONObjectToWKB(yyjson_val *val, WKBWriter &writer) {
	if (!val || !yyjson_is_obj(val)) {
		return false;
	}

	yyjson_val *type_val = yyjson_obj_get(val, "type");
	if (!type_val || !yyjson_is_str(type_val)) {
		return false;
	}
	const char *type_str = yyjson_get_str(type_val);

	if (strcmp(type_str, "Point") == 0) {
		yyjson_val *coords = yyjson_obj_get(val, "coordinates");
		if (!coords || !yyjson_is_arr(coords) || yyjson_arr_size(coords) < 2) {
			return false;
		}
		writer.WriteHeader(WKB_POINT);
		double x = YyjsonGetNum(yyjson_arr_get(coords, 0));
		double y = YyjsonGetNum(yyjson_arr_get(coords, 1));
		writer.WritePoint(x, y);
		return true;
	}

	if (strcmp(type_str, "LineString") == 0) {
		yyjson_val *coords = yyjson_obj_get(val, "coordinates");
		if (!coords || !yyjson_is_arr(coords)) {
			return false;
		}
		writer.WriteHeader(WKB_LINESTRING);
		return WriteGeoJSONCoordinateRing(coords, writer);
	}

	if (strcmp(type_str, "Polygon") == 0) {
		yyjson_val *coords = yyjson_obj_get(val, "coordinates");
		if (!coords || !yyjson_is_arr(coords)) {
			return false;
		}
		writer.WriteHeader(WKB_POLYGON);
		uint32_t nrings = static_cast<uint32_t>(yyjson_arr_size(coords));
		writer.WriteUInt32(nrings);
		size_t idx, max;
		yyjson_val *ring;
		yyjson_arr_foreach(coords, idx, max, ring) {
			if (!WriteGeoJSONCoordinateRing(ring, writer)) {
				return false;
			}
		}
		return true;
	}

	if (strcmp(type_str, "MultiPoint") == 0) {
		yyjson_val *coords = yyjson_obj_get(val, "coordinates");
		if (!coords || !yyjson_is_arr(coords)) {
			return false;
		}
		writer.WriteHeader(WKB_MULTIPOINT);
		uint32_t npoints = static_cast<uint32_t>(yyjson_arr_size(coords));
		writer.WriteUInt32(npoints);
		size_t idx, max;
		yyjson_val *coord;
		yyjson_arr_foreach(coords, idx, max, coord) {
			// Each sub-geometry is a full WKB Point.
			writer.WriteHeader(WKB_POINT);
			if (!WriteGeoJSONCoordinates(coord, writer)) {
				return false;
			}
		}
		return true;
	}

	if (strcmp(type_str, "MultiLineString") == 0) {
		yyjson_val *coords = yyjson_obj_get(val, "coordinates");
		if (!coords || !yyjson_is_arr(coords)) {
			return false;
		}
		writer.WriteHeader(WKB_MULTILINESTRING);
		uint32_t nlines = static_cast<uint32_t>(yyjson_arr_size(coords));
		writer.WriteUInt32(nlines);
		size_t idx, max;
		yyjson_val *line;
		yyjson_arr_foreach(coords, idx, max, line) {
			writer.WriteHeader(WKB_LINESTRING);
			if (!WriteGeoJSONCoordinateRing(line, writer)) {
				return false;
			}
		}
		return true;
	}

	if (strcmp(type_str, "MultiPolygon") == 0) {
		yyjson_val *coords = yyjson_obj_get(val, "coordinates");
		if (!coords || !yyjson_is_arr(coords)) {
			return false;
		}
		writer.WriteHeader(WKB_MULTIPOLYGON);
		uint32_t npolys = static_cast<uint32_t>(yyjson_arr_size(coords));
		writer.WriteUInt32(npolys);
		size_t poly_idx, poly_max;
		yyjson_val *poly;
		yyjson_arr_foreach(coords, poly_idx, poly_max, poly) {
			if (!yyjson_is_arr(poly)) {
				return false;
			}
			writer.WriteHeader(WKB_POLYGON);
			uint32_t nrings = static_cast<uint32_t>(yyjson_arr_size(poly));
			writer.WriteUInt32(nrings);
			size_t ring_idx, ring_max;
			yyjson_val *ring;
			yyjson_arr_foreach(poly, ring_idx, ring_max, ring) {
				if (!WriteGeoJSONCoordinateRing(ring, writer)) {
					return false;
				}
			}
		}
		return true;
	}

	if (strcmp(type_str, "GeometryCollection") == 0) {
		yyjson_val *geometries = yyjson_obj_get(val, "geometries");
		if (!geometries || !yyjson_is_arr(geometries)) {
			return false;
		}
		writer.WriteHeader(WKB_GEOMETRYCOLLECTION);
		uint32_t ngeoms = static_cast<uint32_t>(yyjson_arr_size(geometries));
		writer.WriteUInt32(ngeoms);
		size_t idx, max;
		yyjson_val *geom;
		yyjson_arr_foreach(geometries, idx, max, geom) {
			if (!GeoJSONObjectToWKB(geom, writer)) {
				return false;
			}
		}
		return true;
	}

	// Unknown GeoJSON type.
	return false;
}

// Helper to trim whitespace from string.
static std::string TrimString(const std::string &s) {
	size_t start = s.find_first_not_of(" \t\n\r");
	if (start == std::string::npos)
		return "";
	size_t end = s.find_last_not_of(" \t\n\r");
	return s.substr(start, end - start + 1);
}

// Convert an Elasticsearch geo_point value to WKB Point.
// Handles all 5 input formats:
// 1. object: {"lat": 41.12, "lon": -71.34}
// 2. GeoJSON: {"type": "Point", "coordinates": [-71.34, 41.12]}
// 3. array: [-71.34, 41.12] (lon, lat order)
// 4. string: "41.12,-71.34" (lat, lon order)
// 5. WKT: "POINT (-71.34 41.12)"
static bool GeoPointToWKB(yyjson_val *val, WKBWriter &writer) {
	if (!val) {
		return false;
	}

	double lon = 0, lat = 0;

	if (yyjson_is_obj(val)) {
		// Check for lat/lon object first: {"lat": 41.12, "lon": -71.34}
		yyjson_val *lat_val = yyjson_obj_get(val, "lat");
		yyjson_val *lon_val = yyjson_obj_get(val, "lon");
		if (lat_val && lon_val) {
			lat = YyjsonGetNum(lat_val);
			lon = YyjsonGetNum(lon_val);
		} else {
			// GeoJSON object: {"type": "Point", "coordinates": [lon, lat]}
			yyjson_val *coords = yyjson_obj_get(val, "coordinates");
			if (!coords || !yyjson_is_arr(coords) || yyjson_arr_size(coords) < 2) {
				return false;
			}
			lon = YyjsonGetNum(yyjson_arr_get(coords, 0));
			lat = YyjsonGetNum(yyjson_arr_get(coords, 1));
		}
	} else if (yyjson_is_arr(val)) {
		// Array: [-71.34, 41.12] (lon, lat order)
		if (yyjson_arr_size(val) < 2) {
			return false;
		}
		lon = YyjsonGetNum(yyjson_arr_get(val, 0));
		lat = YyjsonGetNum(yyjson_arr_get(val, 1));
	} else if (yyjson_is_str(val)) {
		const char *str = yyjson_get_str(val);
		std::string s(str);

		// WKT: "POINT (lon lat)" or "POINT(lon lat)"
		std::string trimmed = TrimString(s);
		if (trimmed.find("POINT") == 0) {
			size_t paren_start = trimmed.find('(');
			size_t paren_end = trimmed.rfind(')');
			if (paren_start == std::string::npos || paren_end == std::string::npos || paren_end <= paren_start) {
				return false;
			}
			std::string coords = TrimString(trimmed.substr(paren_start + 1, paren_end - paren_start - 1));
			size_t space_pos = coords.find(' ');
			if (space_pos == std::string::npos) {
				return false;
			}
			try {
				lon = std::stod(coords.substr(0, space_pos));
				lat = std::stod(TrimString(coords.substr(space_pos + 1)));
			} catch (...) {
				return false;
			}
		} else {
			// "lat,lon" string: "41.12,-71.34"
			auto comma_pos = s.find(',');
			if (comma_pos == std::string::npos) {
				return false;
			}
			try {
				lat = std::stod(s.substr(0, comma_pos));
				lon = std::stod(s.substr(comma_pos + 1));
			} catch (...) {
				return false;
			}
		}
	} else {
		return false;
	}

	writer.WriteHeader(WKB_POINT);
	writer.WritePoint(lon, lat);
	return true;
}

// Convert an Elasticsearch geo_shape value to WKB.
// Handles GeoJSON objects and WKT strings.
// For WKT input it uses DuckDB core's Geometry::FromString (WKT -> WKB).
// Returns true on success (out_wkb is set) or false on failure.
static bool GeoShapeToWKB(yyjson_val *val, Vector &result, string_t &out_wkb) {
	if (!val) {
		return false;
	}

	if (yyjson_is_str(val)) {
		// WKT format: use DuckDB core's WKT parser to produce WKB directly.
		const char *str = yyjson_get_str(val);
		string_t input(str, static_cast<uint32_t>(strlen(str)));
		return Geometry::FromString(input, out_wkb, result, false);
	}

	// GeoJSON format: convert to WKB via our GeoJSON to WKB converter.
	WKBWriter writer;
	if (!GeoJSONObjectToWKB(val, writer)) {
		return false;
	}
	out_wkb = writer.Store(result);
	return true;
}

// Reader for little-endian WKB binary data.
class WKBReader {
public:
	WKBReader(const char *data, uint32_t size) : data(data), pos(0), size(size) {
	}

	uint8_t ReadByte() {
		if (pos >= size) {
			return 0;
		}
		return static_cast<uint8_t>(data[pos++]);
	}

	uint32_t ReadUInt32() {
		if (pos + sizeof(uint32_t) > size) {
			return 0;
		}
		uint32_t val;
		memcpy(&val, data + pos, sizeof(val));
		pos += sizeof(val);
		return val;
	}

	double ReadDouble() {
		if (pos + sizeof(double) > size) {
			return 0;
		}
		double val;
		memcpy(&val, data + pos, sizeof(val));
		pos += sizeof(val);
		return val;
	}

private:
	const char *data;
	uint32_t pos;
	uint32_t size;
};

// Forward declaration for recursive WKB to GeoJSON conversion.
static bool WKBToGeoJSONRecursive(WKBReader &reader, yyjson_mut_doc *doc, yyjson_mut_val *parent_arr);

// Read a single WKB geometry and append it as a GeoJSON object to parent_arr.
static bool WKBToGeoJSONRecursive(WKBReader &reader, yyjson_mut_doc *doc, yyjson_mut_val *parent_arr) {
	uint8_t byte_order = reader.ReadByte();
	if (byte_order != 1) {
		return false; // only little-endian supported
	}

	uint32_t type_id = reader.ReadUInt32();

	yyjson_mut_val *geom_obj = yyjson_mut_obj(doc);

	switch (type_id) {
	case WKB_POINT: {
		yyjson_mut_obj_add_str(doc, geom_obj, "type", "Point");
		double x = reader.ReadDouble();
		double y = reader.ReadDouble();
		yyjson_mut_val *coords = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_real(doc, coords, x);
		yyjson_mut_arr_add_real(doc, coords, y);
		yyjson_mut_obj_add_val(doc, geom_obj, "coordinates", coords);
		break;
	}
	case WKB_LINESTRING: {
		yyjson_mut_obj_add_str(doc, geom_obj, "type", "LineString");
		uint32_t npoints = reader.ReadUInt32();
		yyjson_mut_val *coords = yyjson_mut_arr(doc);
		for (uint32_t i = 0; i < npoints; i++) {
			double x = reader.ReadDouble();
			double y = reader.ReadDouble();
			yyjson_mut_val *pt = yyjson_mut_arr(doc);
			yyjson_mut_arr_add_real(doc, pt, x);
			yyjson_mut_arr_add_real(doc, pt, y);
			yyjson_mut_arr_append(coords, pt);
		}
		yyjson_mut_obj_add_val(doc, geom_obj, "coordinates", coords);
		break;
	}
	case WKB_POLYGON: {
		yyjson_mut_obj_add_str(doc, geom_obj, "type", "Polygon");
		uint32_t nrings = reader.ReadUInt32();
		yyjson_mut_val *rings = yyjson_mut_arr(doc);
		for (uint32_t r = 0; r < nrings; r++) {
			uint32_t npoints = reader.ReadUInt32();
			yyjson_mut_val *ring = yyjson_mut_arr(doc);
			for (uint32_t i = 0; i < npoints; i++) {
				double x = reader.ReadDouble();
				double y = reader.ReadDouble();
				yyjson_mut_val *pt = yyjson_mut_arr(doc);
				yyjson_mut_arr_add_real(doc, pt, x);
				yyjson_mut_arr_add_real(doc, pt, y);
				yyjson_mut_arr_append(ring, pt);
			}
			yyjson_mut_arr_append(rings, ring);
		}
		yyjson_mut_obj_add_val(doc, geom_obj, "coordinates", rings);
		break;
	}
	case WKB_MULTIPOINT: {
		yyjson_mut_obj_add_str(doc, geom_obj, "type", "MultiPoint");
		uint32_t ngeoms = reader.ReadUInt32();
		yyjson_mut_val *coords = yyjson_mut_arr(doc);
		for (uint32_t i = 0; i < ngeoms; i++) {
			// Each sub-geometry is a full WKB Point (byte_order + type + x + y).
			reader.ReadByte();   // byte_order
			reader.ReadUInt32(); // type_id (Point=1)
			double x = reader.ReadDouble();
			double y = reader.ReadDouble();
			yyjson_mut_val *pt = yyjson_mut_arr(doc);
			yyjson_mut_arr_add_real(doc, pt, x);
			yyjson_mut_arr_add_real(doc, pt, y);
			yyjson_mut_arr_append(coords, pt);
		}
		yyjson_mut_obj_add_val(doc, geom_obj, "coordinates", coords);
		break;
	}
	case WKB_MULTILINESTRING: {
		yyjson_mut_obj_add_str(doc, geom_obj, "type", "MultiLineString");
		uint32_t ngeoms = reader.ReadUInt32();
		yyjson_mut_val *lines = yyjson_mut_arr(doc);
		for (uint32_t g = 0; g < ngeoms; g++) {
			reader.ReadByte();   // byte_order
			reader.ReadUInt32(); // type_id (LineString=2)
			uint32_t npoints = reader.ReadUInt32();
			yyjson_mut_val *line = yyjson_mut_arr(doc);
			for (uint32_t i = 0; i < npoints; i++) {
				double x = reader.ReadDouble();
				double y = reader.ReadDouble();
				yyjson_mut_val *pt = yyjson_mut_arr(doc);
				yyjson_mut_arr_add_real(doc, pt, x);
				yyjson_mut_arr_add_real(doc, pt, y);
				yyjson_mut_arr_append(line, pt);
			}
			yyjson_mut_arr_append(lines, line);
		}
		yyjson_mut_obj_add_val(doc, geom_obj, "coordinates", lines);
		break;
	}
	case WKB_MULTIPOLYGON: {
		yyjson_mut_obj_add_str(doc, geom_obj, "type", "MultiPolygon");
		uint32_t ngeoms = reader.ReadUInt32();
		yyjson_mut_val *polys = yyjson_mut_arr(doc);
		for (uint32_t g = 0; g < ngeoms; g++) {
			reader.ReadByte();   // byte_order
			reader.ReadUInt32(); // type_id (Polygon=3)
			uint32_t nrings = reader.ReadUInt32();
			yyjson_mut_val *poly = yyjson_mut_arr(doc);
			for (uint32_t r = 0; r < nrings; r++) {
				uint32_t npoints = reader.ReadUInt32();
				yyjson_mut_val *ring = yyjson_mut_arr(doc);
				for (uint32_t i = 0; i < npoints; i++) {
					double x = reader.ReadDouble();
					double y = reader.ReadDouble();
					yyjson_mut_val *pt = yyjson_mut_arr(doc);
					yyjson_mut_arr_add_real(doc, pt, x);
					yyjson_mut_arr_add_real(doc, pt, y);
					yyjson_mut_arr_append(ring, pt);
				}
				yyjson_mut_arr_append(poly, ring);
			}
			yyjson_mut_arr_append(polys, poly);
		}
		yyjson_mut_obj_add_val(doc, geom_obj, "coordinates", polys);
		break;
	}
	case WKB_GEOMETRYCOLLECTION: {
		yyjson_mut_obj_add_str(doc, geom_obj, "type", "GeometryCollection");
		uint32_t ngeoms = reader.ReadUInt32();
		yyjson_mut_val *geometries = yyjson_mut_arr(doc);
		for (uint32_t i = 0; i < ngeoms; i++) {
			if (!WKBToGeoJSONRecursive(reader, doc, geometries)) {
				return false;
			}
		}
		yyjson_mut_obj_add_val(doc, geom_obj, "geometries", geometries);
		break;
	}
	default:
		return false; // unknown geometry type
	}

	yyjson_mut_arr_append(parent_arr, geom_obj);
	return true;
}

// Convert WKB binary to a GeoJSON string.
// Used by filter pushdown to convert GEOMETRY constants (WKB) to GeoJSON for Elasticsearch query DSL.
std::string WKBToGeoJSON(const string_t &wkb) {
	if (wkb.GetSize() == 0) {
		return "";
	}

	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *arr = yyjson_mut_arr(doc);

	WKBReader reader(wkb.GetData(), static_cast<uint32_t>(wkb.GetSize()));
	if (!WKBToGeoJSONRecursive(reader, doc, arr)) {
		yyjson_mut_doc_free(doc);
		return "";
	}

	// Extract the single geometry object from the array.
	yyjson_mut_val *geom = yyjson_mut_arr_get_first(arr);
	if (!geom) {
		yyjson_mut_doc_free(doc);
		return "";
	}

	char *json_str = yyjson_mut_val_write(geom, 0, nullptr);
	yyjson_mut_doc_free(doc);

	if (!json_str) {
		return "";
	}

	std::string result(json_str);
	free(json_str);
	return result;
}

// Check if a WKB Polygon is an axis-aligned rectangle (5 points forming a bbox).
// Used to optimize ST_MakeEnvelope -> geo_bounding_box query in filter pushdown.
// Returns true and fills xmin/ymin/xmax/ymax if the polygon is an axis-aligned rectangle.
bool WKBIsAxisAlignedRectangle(const string_t &wkb, double &xmin, double &ymin, double &xmax, double &ymax) {
	if (wkb.GetSize() < 5) {
		return false;
	}

	WKBReader reader(wkb.GetData(), static_cast<uint32_t>(wkb.GetSize()));
	uint8_t byte_order = reader.ReadByte();
	if (byte_order != 1) {
		return false;
	}

	uint32_t type_id = reader.ReadUInt32();
	if (type_id != WKB_POLYGON) {
		return false;
	}

	uint32_t nrings = reader.ReadUInt32();
	if (nrings != 1) {
		return false;
	}

	uint32_t npoints = reader.ReadUInt32();
	if (npoints != 5) {
		return false;
	}

	// Read the 5 points.
	double xs[5], ys[5];
	for (int i = 0; i < 5; i++) {
		xs[i] = reader.ReadDouble();
		ys[i] = reader.ReadDouble();
	}

	// Closing point must match the first point.
	if (xs[0] != xs[4] || ys[0] != ys[4]) {
		return false;
	}

	// Find bounding box of the 4 corners.
	double env_xmin = xs[0], env_xmax = xs[0];
	double env_ymin = ys[0], env_ymax = ys[0];
	for (int j = 1; j < 4; j++) {
		env_xmin = std::min(env_xmin, xs[j]);
		env_xmax = std::max(env_xmax, xs[j]);
		env_ymin = std::min(env_ymin, ys[j]);
		env_ymax = std::max(env_ymax, ys[j]);
	}

	// Check if all 4 corners lie on the bounding box edges (axis-aligned rectangle).
	for (int j = 0; j < 4; j++) {
		bool on_x_edge = (xs[j] == env_xmin || xs[j] == env_xmax);
		bool on_y_edge = (ys[j] == env_ymin || ys[j] == env_ymax);
		if (!on_x_edge || !on_y_edge) {
			return false;
		}
	}

	xmin = env_xmin;
	ymin = env_ymin;
	xmax = env_xmax;
	ymax = env_ymax;
	return true;
}

yyjson_val *GetValueByPath(yyjson_val *obj, const std::string &path) {
	if (!obj || !yyjson_is_obj(obj))
		return nullptr;

	size_t dot_pos;
	yyjson_val *current = obj;
	std::string remaining = path;

	while ((dot_pos = remaining.find('.')) != std::string::npos) {
		std::string key = remaining.substr(0, dot_pos);
		current = yyjson_obj_get(current, key.c_str());
		if (!current || !yyjson_is_obj(current))
			return nullptr;
		remaining = remaining.substr(dot_pos + 1);
	}

	return yyjson_obj_get(current, remaining.c_str());
}

// Forward declarations for mutual recursion between ConvertJSONToDuckDB, ConvertJSONListToDuckDB and
// ConvertJSONStructToDuckDB.
static void ConvertJSONListToDuckDB(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type,
                                    const std::string &es_type);
static void ConvertJSONStructToDuckDB(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type);

static void ConvertJSONStructToDuckDB(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type) {
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
		ConvertJSONToDuckDB(child_val, *child_entries[i], row_idx, child_type, "");
	}
}

static void ConvertJSONListToDuckDB(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type,
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
		ConvertJSONToDuckDB(val, child_vector, current_size, child_type, es_type);
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
		ConvertJSONToDuckDB(elem, child_vector, current_size + elem_idx, child_type, es_type);
		elem_idx++;
	}
}

void ConvertJSONToDuckDB(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type,
                         const std::string &es_type) {
	if (!val || yyjson_is_null(val)) {
		FlatVector::SetNull(result, row_idx, true);
		return;
	}

	// If the type is LIST, we need to handle it specially. Either the value is an array
	// or it is a single value that should be wrapped in a single-element list.
	// This must be checked before any es_type-specific handling.
	if (type.id() == LogicalTypeId::LIST) {
		ConvertJSONListToDuckDB(val, result, row_idx, type, es_type);
		return;
	}

	// Handle geo_point and geo_shape: convert to native GEOMETRY (WKB binary).
	// The WKB output is stored in the GEOMETRY vector via StringVector::AddStringOrBlob().
	if (es_type == "geo_point" || es_type == "geo_shape") {
		if (es_type == "geo_point") {
			WKBWriter writer;
			if (GeoPointToWKB(val, writer)) {
				FlatVector::GetData<string_t>(result)[row_idx] = writer.Store(result);
			} else {
				FlatVector::SetNull(result, row_idx, true);
			}
		} else {
			string_t wkb;
			if (GeoShapeToWKB(val, result, wkb)) {
				FlatVector::GetData<string_t>(result)[row_idx] = wkb;
			} else {
				FlatVector::SetNull(result, row_idx, true);
			}
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
		ConvertJSONStructToDuckDB(val, result, row_idx, type);
		break;
	default:
		FlatVector::SetNull(result, row_idx, true);
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

	// yyjson_get_real returns 0.0 for integers, need to check for int type.
	if (yyjson_is_int(lon_val)) {
		lon = static_cast<double>(yyjson_get_int(lon_val));
	}
	if (yyjson_is_int(lat_val)) {
		lat = static_cast<double>(yyjson_get_int(lat_val));
	}

	yyjson_doc_free(doc);
	return true;
}

// Convert a DuckDB Value to a yyjson mutable value for Elasticsearch query building.
// Handles all common DuckDB types including numeric, string, date and timestamp.
yyjson_mut_val *ConvertDuckDBToJSON(yyjson_mut_doc *doc, const Value &value) {
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

// Check if an expression references an Elasticsearch geo field.
// With native GEOMETRY columns, this is a direct BOUND_COLUMN_REF with GEOMETRY type
// or a struct_extract chain returning GEOMETRY (for nested geo fields).
bool IsGeoColumnRef(const Expression &expr) {
	// Direct GEOMETRY column reference.
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
	    expr.return_type.id() == LogicalTypeId::GEOMETRY) {
		return true;
	}
	// struct_extract returning GEOMETRY (nested geo field).
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		if (StringUtil::Lower(func_expr.function.name) == "struct_extract" &&
		    expr.return_type.id() == LogicalTypeId::GEOMETRY) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb
