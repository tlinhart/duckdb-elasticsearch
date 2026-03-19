#include "elasticsearch_schema.hpp"
#include "elasticsearch_common.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "yyjson.hpp"

#include <functional>
#include <map>
#include <mutex>

namespace duckdb {

using namespace duckdb_yyjson;

// Structure to hold merged mapping information for a field.
// Used internally during mapping merging for multi-index patterns.
struct MergedFieldInfo {
	LogicalType type;
	std::string es_type;
	std::string first_index; // first index where this field was seen (for error messages)
};

// Result of sampling documents for schema inference.
// Used internally during the sampling phase of schema resolution.
struct SampleResult {
	std::set<std::string> array_fields; // fields detected as containing arrays
	bool has_unmapped_fields;           // whether any unmapped fields were found in the sample
};

// Forward declaration for mutual recursion: BuildStructTypeFromProperties calls BuildDuckDBTypeFromMapping.
static LogicalType BuildDuckDBTypeFromMapping(yyjson_val *field_def);

// Build STRUCT type from Elasticsearch object/nested properties.
static LogicalType BuildStructTypeFromProperties(yyjson_val *properties) {
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

// Build DuckDB type from Elasticsearch field definition.
static LogicalType BuildDuckDBTypeFromMapping(yyjson_val *field_def) {
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
				// Return as native GEOMETRY type (internally WKB). Geo fields are directly usable
				// with spatial functions (ST_Within, ST_Intersects etc.)
				return LogicalType::GEOMETRY();
			}
			return LogicalType::VARCHAR; // default to VARCHAR
		}
	}

	return LogicalType::VARCHAR;
}

// Parse Elasticsearch mapping to extract field names and types.
static void ParseMapping(yyjson_val *properties, const std::string &prefix, vector<string> &column_names,
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

// Recursively collect all field paths from Elasticsearch mapping properties (including nested children).
static void CollectAllMappedPaths(yyjson_val *properties, const std::string &prefix, std::set<std::string> &paths) {
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

// Recursively collect all field paths with their Elasticsearch types from mapping properties.
// This includes nested paths for object/nested types.
static void CollectAllPathTypes(yyjson_val *properties, const std::string &prefix,
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

// Collect text fields that have a .keyword subfield.
// These text fields can be filtered via the .keyword subfield which stores the raw
// (not analyzed) value. Text fields without .keyword cannot be filtered (except IS NULL/IS NOT NULL).
static void CollectTextFieldsWithKeyword(yyjson_val *properties, const std::string &prefix,
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

// Check if two DuckDB types are compatible for merging.
static bool AreTypesCompatible(const LogicalType &type1, const LogicalType &type2) {
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

// Merge two STRUCT types, combining all fields from both.
static LogicalType MergeStructTypes(const LogicalType &type1, const LogicalType &type2) {
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

// Merge mappings from multiple indices, checking for type compatibility.
static void MergeMappingsFromIndices(yyjson_val *root, vector<string> &column_names, vector<LogicalType> &column_types,
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

// Sample documents to detect arrays and unmapped fields.
static SampleResult SampleDocuments(ElasticsearchClient &client, const std::string &index, const std::string &query,
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

	// Fetch sample documents using a plain search (no scroll context needed).
	// The size parameter limits the result to sample_size documents in a single request.
	auto response = client.Search(index, query, sample_size);
	if (!response.success) {
		// If sampling fails, return empty result (conservative: no arrays/unmapped detected).
		return result;
	}

	yyjson_doc *doc = yyjson_read(response.body.c_str(), response.body.size(), 0);
	if (!doc) {
		return result;
	}

	yyjson_val *root = yyjson_doc_get_root(doc);
	yyjson_val *hits_obj = yyjson_obj_get(root, "hits");
	yyjson_val *hits_array = hits_obj ? yyjson_obj_get(hits_obj, "hits") : nullptr;

	if (hits_array && yyjson_is_arr(hits_array)) {
		size_t idx, max;
		yyjson_val *hit;
		yyjson_arr_foreach(hits_array, idx, max, hit) {
			yyjson_val *source = yyjson_obj_get(hit, "_source");
			if (!source) {
				continue;
			}

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

			// Early exit if all arrays and unmapped fields have been detected.
			bool all_arrays_found = (result.array_fields.size() + skip_fields.size() >= field_paths.size());
			if (all_arrays_found && result.has_unmapped_fields) {
				break;
			}
		}
	}

	yyjson_doc_free(doc);

	return result;
}

// Thread-safe per-process cache for resolved Elasticsearch schemas.
// Prevents redundant mapping and sampling HTTP requests when DuckDB calls bind multiple times
// with the same parameters (e.g. UNPIVOT ... ON COLUMNS(*), CTEs referenced multiple times etc.)
class ElasticsearchBindCache {
public:
	static ElasticsearchBindCache &Instance();

	// Look up a cached schema by key. Returns nullptr if not found.
	const ElasticsearchSchema *Get(const string &key);

	// Store a schema in the cache, keyed by the given string.
	void Put(const string &key, ElasticsearchSchema schema);

	// Clear all cached entries. Returns the number of entries that were cleared.
	idx_t Clear();

private:
	ElasticsearchBindCache() = default;

	std::mutex mutex_;
	std::unordered_map<string, ElasticsearchSchema> cache_;
};

ElasticsearchBindCache &ElasticsearchBindCache::Instance() {
	static ElasticsearchBindCache instance;
	return instance;
}

const ElasticsearchSchema *ElasticsearchBindCache::Get(const string &key) {
	lock_guard<mutex> lock(mutex_);
	auto it = cache_.find(key);
	if (it != cache_.end()) {
		return &it->second;
	}
	return nullptr;
}

void ElasticsearchBindCache::Put(const string &key, ElasticsearchSchema schema) {
	lock_guard<mutex> lock(mutex_);
	cache_[key] = std::move(schema);
}

idx_t ElasticsearchBindCache::Clear() {
	lock_guard<mutex> lock(mutex_);
	idx_t count = cache_.size();
	cache_.clear();
	return count;
}

// Build a cache key from the resolved Elasticsearch configuration and query parameters.
// Includes parameters that affect the schema: host, port, index, base query and sample size.
static string BuildBindCacheKey(const ElasticsearchConfig &config, const string &index, const string &base_query,
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
	key += to_string(sample_size);
	return key;
}

// Resolve schema for an Elasticsearch index, with caching.
// Fetches the index mapping and samples documents on cache miss; returns cached data on cache hit.
// Returns an ElasticsearchSchema containing all mapping and sampling information.
ElasticsearchSchema ResolveElasticsearchSchema(const ElasticsearchConfig &config, const string &index,
                                               const string &base_query, int64_t sample_size,
                                               shared_ptr<Logger> logger) {
	// Check cache first.
	string cache_key = BuildBindCacheKey(config, index, base_query, sample_size);
	auto &cache = ElasticsearchBindCache::Instance();
	const auto *cached = cache.Get(cache_key);
	if (cached) {
		// Return a copy of the cached schema.
		return *cached;
	}

	// Cache miss: fetch mapping and sample documents from Elasticsearch.
	ElasticsearchSchema result;

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
	MergeMappingsFromIndices(root, result.column_names, result.column_types, result.field_paths, result.es_types,
	                         result.all_mapped_paths);

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
	for (size_t i = 0; i < result.column_names.size(); i++) {
		const string &col_name = result.column_names[i];
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
				if (result.column_types[i].id() != LogicalTypeId::LIST) {
					result.column_types[i] = LogicalType::LIST(result.column_types[i]);
				}
			}
		}
	}

	// Store a copy in cache for subsequent bind calls with the same parameters.
	cache.Put(cache_key, result);

	return result;
}

void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter) {
	ElasticsearchBindCache::Instance().Clear();
}

// Scalar function that clears the per-process schema cache and returns true on success.
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
