#include "elasticsearch_query.hpp"
#include "elasticsearch_common.hpp"
#include "elasticsearch_filter_pushdown.hpp"
#include "elasticsearch_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "yyjson.hpp"

#include <functional>
#include <set>

namespace duckdb {

using namespace duckdb_yyjson;

// Bind data for the elasticsearch_query function.
// Contains connection config, schema, base query and all query-specific parameters.
struct ElasticsearchQueryBindData : public TableFunctionData {
	ElasticsearchConfig config;
	std::string index;
	std::string base_query; // user-provided query (optional, merged with filters)

	// Logger for HTTP request logging, captured from ClientContext during bind.
	shared_ptr<Logger> logger;

	// Resolved Elasticsearch schema containing all mapping and sampling information.
	// Produced by ResolveElasticsearchSchema() and consumed by query building, filter pushdown and scanning.
	ElasticsearchSchema schema;

	// Sample size for array detection (0 = disabled).
	// Populated from elasticsearch_sample_size setting, overridable by named parameter.
	int64_t sample_size;

	// Scroll and batch settings.
	// Populated from extension settings, not overridable by named parameters.
	int64_t batch_size;                  // from elasticsearch_batch_size
	int64_t batch_size_threshold_factor; // from elasticsearch_batch_size_threshold_factor
	std::string scroll_time;             // from elasticsearch_scroll_time

	// Limit pushdown values (set by optimizer extension).
	// -1 means no limit, 0 means no offset.
	int64_t limit = -1;
	int64_t offset = 0;
};

// Projected subset of schema information for the columns actually needed during scanning.
// Built during init from the full ElasticsearchSchema by selecting only the projected columns.
struct ProjectedSchema {
	// Indices into the bind schema's column layout: [_id (0), ...fields... (1 to N), _unmapped_ (N+1)].
	vector<idx_t> column_indices;

	// Elasticsearch field paths for projected columns (for reading values from _source).
	vector<string> field_paths;

	// Elasticsearch type strings for projected columns (for special type handling during scanning).
	vector<string> es_types;

	// DuckDB types for projected columns (for value extraction and type conversion).
	vector<LogicalType> column_types;
};

// Global state for scanning.
struct ElasticsearchQueryGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<ElasticsearchClient> client;
	std::string scroll_id;
	bool finished;
	idx_t current_row;

	// Parsed documents from current scroll batch.
	vector<yyjson_doc *> docs;
	vector<yyjson_val *> hits;
	idx_t current_hit_idx;

	// Total rows to return (from limit pushdown).
	int64_t max_rows;

	// Offset handling for OFFSET pushdown.
	int64_t rows_to_skip;
	int64_t rows_skipped;

	// Projected subset of schema information for the columns needed during scanning.
	// Built during init from the full ElasticsearchSchema by selecting only the projected columns.
	ProjectedSchema projected;

	// The final query sent to Elasticsearch (with filters merged).
	std::string final_query;

	ElasticsearchQueryGlobalState()
	    : finished(false), current_row(0), current_hit_idx(0), max_rows(-1), rows_to_skip(0), rows_skipped(0) {
	}

	~ElasticsearchQueryGlobalState() {
		for (auto doc : docs) {
			if (doc)
				yyjson_doc_free(doc);
		}
		// Clear scroll if active.
		if (client && !scroll_id.empty()) {
			client->ClearScroll(scroll_id);
		}
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Build the final Elasticsearch query by merging base query with pushed filters and projection.
// projection_ids contains indices into column_ids for columns that need to be in the output.
// If projection_ids is empty, all column_ids are output columns. Otherwise, columns not in
// projection_ids are filter-only columns and can be excluded from _source since Elasticsearch
// handles filtering server-side.
static std::string BuildElasticsearchQuery(const ElasticsearchQueryBindData &bind_data, const TableFilterSet *filters,
                                           const vector<idx_t> &column_ids, const vector<idx_t> &projection_ids) {
	std::string result;

	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	yyjson_mut_val *query_clause = nullptr;
	yyjson_mut_val *base_query_clause = nullptr;

	// Parse base query if provided (the query parameter is the query clause).
	if (!bind_data.base_query.empty()) {
		yyjson_doc *base_doc = yyjson_read(bind_data.base_query.c_str(), bind_data.base_query.size(), 0);
		if (base_doc) {
			yyjson_val *base_root = yyjson_doc_get_root(base_doc);
			base_query_clause = yyjson_val_mut_copy(doc, base_root);
			yyjson_doc_free(base_doc);
		}
	}

	// Translate pushed filters to Elasticsearch Query DSL.
	// IS NULL / IS NOT NULL filters are handled through table_filters (added by pushdown_complex_filter).
	// Filters on _id (IS NOT NULL and IS NULL) are normally optimized away by the optimizer extension
	// (OptimizeIdFilters) before physical plan creation. TranslateFilters also skips them as
	// defense-in-depth.
	yyjson_mut_val *filter_clause = nullptr;
	if (filters && filters->HasFilters()) {
		// Build column names vector for the filter translator.
		// Important: Filter indices in TableFilterSet are relative to column_ids (the projected columns)
		// and not the original bind schema, we need to map them correctly.
		//
		// column_ids contains indices into the bind schema:
		// [_id (0), ...fields... (1 to N), _unmapped_ (N+1)]
		// Filter indices are positions within column_ids.
		vector<string> filter_column_names;
		for (idx_t col_id : column_ids) {
			if (col_id == 0) {
				filter_column_names.push_back("_id");
			} else if (col_id <= bind_data.schema.column_names.size()) {
				// regular field column (col_id 1 maps to column_names[0] etc.)
				const string &name = bind_data.schema.column_names[col_id - 1];
				filter_column_names.push_back(name);
			} else {
				// _unmapped_ column
				filter_column_names.push_back("_unmapped_");
			}
		}

		FilterTranslationResult translation_result =
		    TranslateFilters(doc, *filters, filter_column_names, bind_data.schema);

		filter_clause = translation_result.es_query;
	}

	// Merge base query and filter clause.
	if (base_query_clause && filter_clause) {
		// Both exist, combine with bool.must.
		yyjson_mut_val *bool_obj = yyjson_mut_obj(doc);
		yyjson_mut_val *must_arr = yyjson_mut_arr(doc);
		yyjson_mut_arr_append(must_arr, base_query_clause);
		yyjson_mut_arr_append(must_arr, filter_clause);
		yyjson_mut_obj_add_val(doc, bool_obj, "must", must_arr);

		query_clause = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, query_clause, "bool", bool_obj);
	} else if (base_query_clause) {
		query_clause = base_query_clause;
	} else if (filter_clause) {
		query_clause = filter_clause;
	} else {
		// No query, use match_all.
		query_clause = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, query_clause, "match_all", yyjson_mut_obj(doc));
	}

	yyjson_mut_obj_add_val(doc, root, "query", query_clause);

	// Add _source projection if we have specific columns.
	// Column layout: [_id, ...fields..., _unmapped_].
	// We need to request only the field paths for output columns (not filter-only columns).
	//
	// projection_ids contains indices into column_ids for columns that need to be in the output.
	// If projection_ids is empty, all column_ids are output columns (no filter-only columns).
	// Otherwise, columns at indices not in projection_ids are filter-only and can be excluded
	// from _source since Elasticsearch handles filtering server-side.
	//
	// Important: If _unmapped_ column is projected, we need the full _source to detect unmapped fields.

	// Build set of output column indices (indices into column_ids that are actual output).
	std::set<idx_t> output_column_indices;
	if (projection_ids.empty()) {
		// No filter-only columns, all column_ids are output columns.
		for (idx_t i = 0; i < column_ids.size(); i++) {
			output_column_indices.insert(i);
		}
	} else {
		// Only columns at projection_ids indices are output columns.
		for (idx_t proj_id : projection_ids) {
			output_column_indices.insert(proj_id);
		}
	}

	bool needs_full_source = false;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		// Skip filter-only columns when checking for _unmapped_.
		if (output_column_indices.count(i) == 0) {
			continue;
		}
		idx_t col_id = column_ids[i];
		// _unmapped_ column is always at position field_paths.size() + 1 (after _id and all fields).
		if (col_id > bind_data.schema.field_paths.size()) {
			needs_full_source = true;
			break;
		}
	}

	if (!column_ids.empty() && !needs_full_source) {
		vector<string> source_fields;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			// Skip filter-only columns (not in output).
			if (output_column_indices.count(i) == 0) {
				continue;
			}
			idx_t col_id = column_ids[i];
			// Skip _id (col 0), it's always returned by Elasticsearch.
			if (col_id == 0) {
				continue;
			}
			idx_t field_idx = col_id - 1; // adjust for _id column
			if (field_idx < bind_data.schema.field_paths.size()) {
				source_fields.push_back(bind_data.schema.field_paths[field_idx]);
			}
		}

		if (!source_fields.empty()) {
			yyjson_mut_val *source_arr = yyjson_mut_arr(doc);
			for (const auto &field : source_fields) {
				yyjson_mut_arr_add_strcpy(doc, source_arr, field.c_str());
			}
			yyjson_mut_obj_add_val(doc, root, "_source", source_arr);
		}
	}
	// If needs_full_source is true, we do not set _source, so Elasticsearch returns the full document.

	// Note: We do not add "size" to the query body here. For scroll API, the batch size is controlled
	// by the URL parameter in ScrollSearch(). Adding "size" to the body would be misleading since
	// Elasticsearch ignores it for scroll requests.

	// Serialize the query.
	char *json_str = yyjson_mut_write(doc, 0, nullptr);
	if (json_str) {
		result = json_str;
		free(json_str);
	}

	yyjson_mut_doc_free(doc);
	return result;
}

// Bind function, called to determine output schema.
static unique_ptr<FunctionData> ElasticsearchQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<ElasticsearchQueryBindData>();

	// Capture logger from ClientContext if HTTP logging is enabled.
	auto &client_config = ClientConfig::GetConfig(context);
	if (client_config.enable_http_logging) {
		bind_data->logger = context.logger;
	}

	// Initialize defaults for parameters that are not backed by extension settings.
	bind_data->config.host = "localhost";
	bind_data->config.port = 9200;
	bind_data->config.use_ssl = false;

	// Initialize defaults from extension settings.
	// These are the single source of truth for default values (registered in LoadInternal).
	Value setting_val;
	if (context.TryGetCurrentSetting("elasticsearch_verify_ssl", setting_val)) {
		bind_data->config.verify_ssl = BooleanValue::Get(setting_val);
	}
	if (context.TryGetCurrentSetting("elasticsearch_timeout", setting_val)) {
		bind_data->config.timeout = IntegerValue::Get(setting_val);
	}
	if (context.TryGetCurrentSetting("elasticsearch_max_retries", setting_val)) {
		bind_data->config.max_retries = IntegerValue::Get(setting_val);
	}
	if (context.TryGetCurrentSetting("elasticsearch_retry_interval", setting_val)) {
		bind_data->config.retry_interval = IntegerValue::Get(setting_val);
	}
	if (context.TryGetCurrentSetting("elasticsearch_retry_backoff_factor", setting_val)) {
		bind_data->config.retry_backoff_factor = DoubleValue::Get(setting_val);
	}
	if (context.TryGetCurrentSetting("elasticsearch_sample_size", setting_val)) {
		bind_data->sample_size = IntegerValue::Get(setting_val);
	}

	// Parse named parameters (override settings when explicitly specified).
	for (auto &kv : input.named_parameters) {
		if (kv.first == "host") {
			bind_data->config.host = StringValue::Get(kv.second);
		} else if (kv.first == "port") {
			bind_data->config.port = IntegerValue::Get(kv.second);
		} else if (kv.first == "index") {
			bind_data->index = StringValue::Get(kv.second);
		} else if (kv.first == "query") {
			bind_data->base_query = StringValue::Get(kv.second);
		} else if (kv.first == "username") {
			bind_data->config.username = StringValue::Get(kv.second);
		} else if (kv.first == "password") {
			bind_data->config.password = StringValue::Get(kv.second);
		} else if (kv.first == "use_ssl") {
			bind_data->config.use_ssl = BooleanValue::Get(kv.second);
		} else if (kv.first == "verify_ssl") {
			bind_data->config.verify_ssl = BooleanValue::Get(kv.second);
		} else if (kv.first == "timeout") {
			bind_data->config.timeout = IntegerValue::Get(kv.second);
		} else if (kv.first == "max_retries") {
			bind_data->config.max_retries = IntegerValue::Get(kv.second);
		} else if (kv.first == "retry_interval") {
			bind_data->config.retry_interval = IntegerValue::Get(kv.second);
		} else if (kv.first == "retry_backoff_factor") {
			bind_data->config.retry_backoff_factor = DoubleValue::Get(kv.second);
		} else if (kv.first == "sample_size") {
			bind_data->sample_size = IntegerValue::Get(kv.second);
		}
	}

	// Validate required parameters.
	if (bind_data->config.host.empty()) {
		throw InvalidInputException("elasticsearch_query requires 'host' parameter");
	}
	if (bind_data->index.empty()) {
		throw InvalidInputException("elasticsearch_query requires 'index' parameter");
	}

	// Read proxy configuration from DuckDB's core settings.
	bind_data->config.proxy_host = Settings::Get<HTTPProxySetting>(context);
	bind_data->config.proxy_username = Settings::Get<HTTPProxyUsernameSetting>(context);
	bind_data->config.proxy_password = Settings::Get<HTTPProxyPasswordSetting>(context);

	// Resolve schema from Elasticsearch (mapping + document sampling), with caching.
	// On cache hit, no HTTP requests are made. On cache miss, the mapping is fetched and
	// documents are sampled to detect arrays and unmapped fields.
	bind_data->schema = ResolveElasticsearchSchema(bind_data->config, bind_data->index, bind_data->base_query,
	                                               bind_data->sample_size, bind_data->logger);

	// Read query-specific extension settings.
	if (context.TryGetCurrentSetting("elasticsearch_batch_size", setting_val)) {
		bind_data->batch_size = IntegerValue::Get(setting_val);
	}
	if (context.TryGetCurrentSetting("elasticsearch_batch_size_threshold_factor", setting_val)) {
		bind_data->batch_size_threshold_factor = IntegerValue::Get(setting_val);
	}
	if (context.TryGetCurrentSetting("elasticsearch_scroll_time", setting_val)) {
		bind_data->scroll_time = StringValue::Get(setting_val);
	}

	// Build output schema: [_id, ...fields..., _unmapped_].
	names.push_back("_id");
	return_types.push_back(LogicalType::VARCHAR);

	for (size_t i = 0; i < bind_data->schema.column_names.size(); i++) {
		names.push_back(bind_data->schema.column_names[i]);
		return_types.push_back(bind_data->schema.column_types[i]);
	}

	// Always add _unmapped_ column to capture fields not in the mapping.
	names.push_back("_unmapped_");
	return_types.push_back(LogicalType::VARIANT());

	return std::move(bind_data);
}

// Initialize global state with pushdown handling.
static unique_ptr<GlobalTableFunctionState> ElasticsearchQueryInitGlobal(ClientContext &context,
                                                                         TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ElasticsearchQueryBindData>();
	auto state = make_uniq<ElasticsearchQueryGlobalState>();

	// Handle projection pushdown with filter pruning.
	// column_ids contains indices of all columns needed (output + filter-only).
	// projection_ids contains indices into column_ids for output columns only.
	// If projection_ids is empty, all column_ids are output columns (no filter pruning).
	//
	// We populate the projected schema only for output columns, since:
	// 1. filter-only columns are excluded from _source (Elasticsearch filters server-side)
	// 2. the output DataChunk has projection_ids.size() columns (or column_ids.size() if empty)
	// 3. we write directly to output.data[i] where i corresponds to output column index

	bool has_filter_prune = !input.projection_ids.empty() && input.projection_ids.size() < input.column_ids.size();

	if (has_filter_prune) {
		// Filter pruning is active, only build metadata for output columns.
		// projected.column_indices will contain the actual column IDs for output columns.
		for (idx_t proj_idx : input.projection_ids) {
			idx_t col_id = input.column_ids[proj_idx];
			state->projected.column_indices.push_back(col_id);

			if (col_id == 0) {
				// _id column
				state->projected.field_paths.push_back("_id");
				state->projected.es_types.push_back("");
				state->projected.column_types.push_back(LogicalType::VARCHAR);
			} else if (col_id <= bind_data.schema.field_paths.size()) {
				// regular field column
				idx_t field_idx = col_id - 1;
				state->projected.field_paths.push_back(bind_data.schema.field_paths[field_idx]);
				state->projected.es_types.push_back(bind_data.schema.es_types[field_idx]);
				state->projected.column_types.push_back(bind_data.schema.column_types[field_idx]);
			} else {
				// _unmapped_ column
				state->projected.field_paths.push_back("_unmapped_");
				state->projected.es_types.push_back("");
				state->projected.column_types.push_back(LogicalType::VARIANT());
			}
		}
	} else {
		// No filter pruning: all column_ids are output columns.
		state->projected.column_indices = input.column_ids;

		// Build projected field info for all columns.
		// Column layout: [_id (0), ...fields... (1 to N), _unmapped_ (N+1)].
		for (idx_t col_id : input.column_ids) {
			if (col_id == 0) {
				// _id column
				state->projected.field_paths.push_back("_id");
				state->projected.es_types.push_back("");
				state->projected.column_types.push_back(LogicalType::VARCHAR);
			} else if (col_id <= bind_data.schema.field_paths.size()) {
				// regular field column
				idx_t field_idx = col_id - 1;
				state->projected.field_paths.push_back(bind_data.schema.field_paths[field_idx]);
				state->projected.es_types.push_back(bind_data.schema.es_types[field_idx]);
				state->projected.column_types.push_back(bind_data.schema.column_types[field_idx]);
			} else {
				// _unmapped_ column
				state->projected.field_paths.push_back("_unmapped_");
				state->projected.es_types.push_back("");
				state->projected.column_types.push_back(LogicalType::VARIANT());
			}
		}
	}

	// Use limit and offset from bind_data (set by optimizer extension).
	state->max_rows = bind_data.limit;
	state->rows_to_skip = bind_data.offset;
	state->rows_skipped = 0;

	// Calculate the query limit. We need to fetch limit + offset rows from Elasticsearch,
	// then skip the first offset rows and return the next limit rows.
	int64_t query_limit = bind_data.limit;
	if (bind_data.limit > 0 && bind_data.offset > 0) {
		query_limit = bind_data.limit + bind_data.offset;
	}

	// Build the final query with pushdown.
	state->final_query =
	    BuildElasticsearchQuery(bind_data, input.filters.get(), input.column_ids, input.projection_ids);

	// Create client.
	state->client = make_uniq<ElasticsearchClient>(bind_data.config, bind_data.logger);

	// Determine batch size. For small query limits, fetch all needed rows in one request
	// when the total is within the threshold (batch_size * threshold_factor).
	// For larger limits, keep the configured batch size to avoid memory issues.
	int64_t batch_size = bind_data.batch_size;
	int64_t batch_threshold = bind_data.batch_size * bind_data.batch_size_threshold_factor;
	if (query_limit > 0 && query_limit <= batch_threshold) {
		batch_size = query_limit;
	}

	// Start scroll search.
	auto response = state->client->ScrollSearch(bind_data.index, state->final_query, bind_data.scroll_time, batch_size);

	if (!response.success) {
		throw IOException("Elasticsearch search failed: " + response.error_message);
	}

	// Parse response.
	yyjson_doc *doc = yyjson_read(response.body.c_str(), response.body.size(), 0);
	if (!doc) {
		throw IOException("Failed to parse Elasticsearch response");
	}

	state->docs.push_back(doc);

	yyjson_val *root = yyjson_doc_get_root(doc);
	yyjson_val *scroll_id_val = yyjson_obj_get(root, "_scroll_id");
	if (scroll_id_val) {
		state->scroll_id = yyjson_get_str(scroll_id_val);
	}

	// Get hits.
	yyjson_val *hits_obj = yyjson_obj_get(root, "hits");
	if (hits_obj) {
		yyjson_val *hits_array = yyjson_obj_get(hits_obj, "hits");
		if (hits_array && yyjson_is_arr(hits_array)) {
			size_t idx, max;
			yyjson_val *hit;
			yyjson_arr_foreach(hits_array, idx, max, hit) {
				state->hits.push_back(hit);
			}
		}
	}

	if (state->hits.empty()) {
		state->finished = true;
	}

	return std::move(state);
}

// Collect unmapped fields from _source that are not in the schema's mapped paths.
// Returns a VariantValue of unmapped fields or a null VariantValue if none found.
// Used to populate the _unmapped_ output column during scanning.
static VariantValue CollectUnmappedFields(yyjson_val *source, const std::set<std::string> &mapped_paths,
                                          const std::string &prefix = "") {
	if (!source || !yyjson_is_obj(source)) {
		return VariantValue(Value());
	}

	bool has_unmapped = false;
	VariantValue result_obj(VariantValueType::OBJECT);

	// Recursive helper to collect unmapped fields.
	std::function<void(yyjson_val *, VariantValue &, const std::string &)> collect_unmapped =
	    [&](yyjson_val *obj, VariantValue &target, const std::string &current_prefix) {
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
				    // If the field has no children in mapped_paths, it's a terminal type (geo_point etc.)
				    // and we should not recurse into it even if the value is an object.
				    bool has_mapped_children = false;
				    for (const auto &mp : mapped_paths) {
					    if (mp.find(field_path + ".") == 0) {
						    has_mapped_children = true;
						    break;
					    }
				    }

				    if (has_mapped_children && yyjson_is_obj(field_val)) {
					    // This is an object/nested type with defined child fields, check for unmapped children.
					    VariantValue sub_obj(VariantValueType::OBJECT);
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
								    sub_obj.AddChild(subfield_name, ConvertYyjsonToVariantValue(subfield_val));
								    sub_has_unmapped = true;
							    } else {
								    // Recurse into this object.
								    VariantValue nested_obj(VariantValueType::OBJECT);
								    collect_unmapped(subfield_val, nested_obj, subfield_path);
								    if (!nested_obj.object_children.empty()) {
									    sub_obj.AddChild(subfield_name, std::move(nested_obj));
									    sub_has_unmapped = true;
								    }
							    }
						    } else if (yyjson_is_obj(subfield_val)) {
							    // Recurse for nested mapped objects.
							    VariantValue nested_obj(VariantValueType::OBJECT);
							    collect_unmapped(subfield_val, nested_obj, subfield_path);
							    if (!nested_obj.object_children.empty()) {
								    sub_obj.AddChild(subfield_name, std::move(nested_obj));
								    sub_has_unmapped = true;
							    }
						    }
					    }

					    if (sub_has_unmapped) {
						    target.AddChild(field_name, std::move(sub_obj));
						    has_unmapped = true;
					    }
				    }
				    // Terminal type (geo_point, keyword etc.), do not recurse.
			    } else if (is_parent_of_mapped) {
				    // This is a parent object of mapped fields, recurse to find unmapped children.
				    if (yyjson_is_obj(field_val)) {
					    VariantValue sub_obj(VariantValueType::OBJECT);
					    collect_unmapped(field_val, sub_obj, field_path);
					    if (!sub_obj.object_children.empty()) {
						    target.AddChild(field_name, std::move(sub_obj));
						    has_unmapped = true;
					    }
				    }
			    } else {
				    // This field is completely unmapped, add the entire value.
				    target.AddChild(field_name, ConvertYyjsonToVariantValue(field_val));
				    has_unmapped = true;
			    }
		    }
	    };

	collect_unmapped(source, result_obj, prefix);

	if (has_unmapped) {
		return result_obj;
	}
	return VariantValue(Value());
}

// Main scan function.
static void ElasticsearchQueryScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<ElasticsearchQueryBindData>();
	auto &state = data.global_state->Cast<ElasticsearchQueryGlobalState>();

	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	// Check if we've hit the limit.
	if (state.max_rows > 0 && static_cast<int64_t>(state.current_row) >= state.max_rows) {
		state.finished = true;
		output.SetCardinality(0);
		return;
	}

	idx_t output_idx = 0;
	idx_t max_output = STANDARD_VECTOR_SIZE;

	// Adjust max_output if we have a limit.
	if (state.max_rows > 0) {
		int64_t remaining = state.max_rows - static_cast<int64_t>(state.current_row);
		if (remaining < static_cast<int64_t>(max_output)) {
			max_output = static_cast<idx_t>(remaining);
		}
	}

	// Detect if _unmapped_ column is projected and track its output column index.
	// VariantValue objects are collected per row and written to the output vector after the scan loop.
	idx_t unmapped_out_col = DConstants::INVALID_INDEX;
	for (idx_t out_col = 0; out_col < state.projected.field_paths.size(); out_col++) {
		if (state.projected.field_paths[out_col] == "_unmapped_") {
			unmapped_out_col = out_col;
			break;
		}
	}
	vector<VariantValue> unmapped_values;
	if (unmapped_out_col != DConstants::INVALID_INDEX) {
		unmapped_values.reserve(max_output);
	}

	while (output_idx < max_output && !state.finished) {
		// Check if we need more data.
		if (state.current_hit_idx >= state.hits.size()) {
			// Check if we've hit the limit.
			if (state.max_rows > 0 && static_cast<int64_t>(state.current_row) >= state.max_rows) {
				state.finished = true;
				break;
			}

			// Fetch next scroll batch.
			if (state.scroll_id.empty()) {
				state.finished = true;
				break;
			}

			auto response = state.client->ScrollNext(state.scroll_id, bind_data.scroll_time);
			if (!response.success) {
				throw IOException("Elasticsearch scroll failed: " + response.error_message);
			}

			// Clear old docs and hits.
			for (auto doc : state.docs) {
				if (doc)
					yyjson_doc_free(doc);
			}
			state.docs.clear();
			state.hits.clear();

			// Parse new response.
			yyjson_doc *doc = yyjson_read(response.body.c_str(), response.body.size(), 0);
			if (!doc) {
				throw IOException("Failed to parse Elasticsearch scroll response");
			}
			state.docs.push_back(doc);

			yyjson_val *root = yyjson_doc_get_root(doc);
			yyjson_val *hits_obj = yyjson_obj_get(root, "hits");
			if (hits_obj) {
				yyjson_val *hits_array = yyjson_obj_get(hits_obj, "hits");
				if (hits_array && yyjson_is_arr(hits_array)) {
					size_t idx, max;
					yyjson_val *hit;
					yyjson_arr_foreach(hits_array, idx, max, hit) {
						state.hits.push_back(hit);
					}
				}
			}

			state.current_hit_idx = 0;

			if (state.hits.empty()) {
				state.finished = true;
				break;
			}
		}

		// Handle OFFSET (skip rows until we've skipped enough).
		if (state.rows_skipped < state.rows_to_skip) {
			state.current_hit_idx++;
			state.rows_skipped++;
			continue;
		}

		// Process current hit.
		yyjson_val *hit = state.hits[state.current_hit_idx];
		yyjson_val *source = yyjson_obj_get(hit, "_source");
		yyjson_val *id_val = yyjson_obj_get(hit, "_id");

		// Process each projected column.
		for (idx_t out_col = 0; out_col < state.projected.column_indices.size(); out_col++) {
			idx_t col_id = state.projected.column_indices[out_col];
			const std::string &field_path = state.projected.field_paths[out_col];
			const std::string &es_type = state.projected.es_types[out_col];
			const LogicalType &col_type = state.projected.column_types[out_col];

			if (col_id == 0) {
				// _id column
				if (id_val && yyjson_is_str(id_val)) {
					auto str_val = StringVector::AddString(output.data[out_col], yyjson_get_str(id_val));
					FlatVector::GetData<string_t>(output.data[out_col])[output_idx] = str_val;
				} else {
					FlatVector::SetNull(output.data[out_col], output_idx, true);
				}
			} else if (field_path == "_unmapped_") {
				// _unmapped_ column: collect VariantValue written to output after the scan loop.
				VariantValue unmapped = CollectUnmappedFields(source, bind_data.schema.all_mapped_paths);
				unmapped_values.push_back(std::move(unmapped));
			} else {
				// regular field
				yyjson_val *val = GetValueByPath(source, field_path);
				ConvertJSONToDuckDB(val, output.data[out_col], output_idx, col_type, es_type);
			}
		}

		output_idx++;
		state.current_hit_idx++;
		state.current_row++;
	}

	// Write collected VariantValues to the _unmapped_ output column.
	if (unmapped_out_col != DConstants::INVALID_INDEX && output_idx > 0) {
		VariantValue::ToVARIANT(unmapped_values, output.data[unmapped_out_col]);
	}

	output.SetCardinality(output_idx);
}

// Thin adapter: casts bind data to ElasticsearchQueryBindData, creates ElasticsearchPushdownContext,
// and delegates to the shared PushdownComplexFilters implementation in elasticsearch_filter_pushdown.cpp.
static void ElasticsearchQueryPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                                    vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<ElasticsearchQueryBindData>();
	ElasticsearchPushdownContext pushdown_ctx {bind_data.schema};
	PushdownComplexFilters(context, get, pushdown_ctx, filters);
}

// Helper function for the optimizer extension to set limit/offset in bind data.
// Called from elasticsearch_optimizer.cpp after verifying the function name is "elasticsearch_query".
void SetElasticsearchLimitOffset(FunctionData &bind_data, int64_t limit, int64_t offset) {
	auto &es_bind_data = bind_data.Cast<ElasticsearchQueryBindData>();
	es_bind_data.limit = limit;
	es_bind_data.offset = offset;
}

void RegisterElasticsearchQueryFunction(ExtensionLoader &loader) {
	TableFunction elasticsearch_query("elasticsearch_query", {}, ElasticsearchQueryScan, ElasticsearchQueryBind,
	                                  ElasticsearchQueryInitGlobal);

	// Enable pushdown.
	elasticsearch_query.projection_pushdown = true;
	elasticsearch_query.filter_pushdown = true;
	elasticsearch_query.filter_prune = true;
	elasticsearch_query.pushdown_complex_filter = ElasticsearchQueryPushdownComplexFilter;

	// Named parameters.
	elasticsearch_query.named_parameters["host"] = LogicalType::VARCHAR;
	elasticsearch_query.named_parameters["port"] = LogicalType::INTEGER;
	elasticsearch_query.named_parameters["index"] = LogicalType::VARCHAR;
	elasticsearch_query.named_parameters["query"] = LogicalType::VARCHAR;
	elasticsearch_query.named_parameters["username"] = LogicalType::VARCHAR;
	elasticsearch_query.named_parameters["password"] = LogicalType::VARCHAR;
	elasticsearch_query.named_parameters["use_ssl"] = LogicalType::BOOLEAN;
	elasticsearch_query.named_parameters["verify_ssl"] = LogicalType::BOOLEAN;
	elasticsearch_query.named_parameters["timeout"] = LogicalType::INTEGER;
	elasticsearch_query.named_parameters["max_retries"] = LogicalType::INTEGER;
	elasticsearch_query.named_parameters["retry_interval"] = LogicalType::INTEGER;
	elasticsearch_query.named_parameters["retry_backoff_factor"] = LogicalType::DOUBLE;
	elasticsearch_query.named_parameters["sample_size"] = LogicalType::INTEGER;

	loader.RegisterFunction(elasticsearch_query);
}

} // namespace duckdb
