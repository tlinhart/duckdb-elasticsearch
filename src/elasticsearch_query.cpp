#include "elasticsearch_query.hpp"
#include "elasticsearch_common.hpp"
#include "elasticsearch_filter_translator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "yyjson.hpp"

#include <algorithm>
#include <functional>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

using namespace duckdb_yyjson;

// Bind data for the elasticsearch_query function.
struct ElasticsearchQueryBindData : public TableFunctionData {
	ElasticsearchConfig config;
	std::string index;
	std::string base_query; // user-provided query (optional, merged with filters)

	// Logger for HTTP request logging, captured from ClientContext during bind.
	shared_ptr<Logger> logger;

	// Schema information (all columns from the mapping).
	vector<string> all_column_names;
	vector<LogicalType> all_column_types;

	// For storing the Elasticsearch field paths (may differ from column names for nested fields).
	vector<string> field_paths;

	// For storing all mapped field paths including nested (for unmapped detection).
	std::set<string> all_mapped_paths;

	// Store Elasticsearch types for special handling (geo types, text fields).
	vector<string> es_types;

	// Map from column name to Elasticsearch type (for filter translation).
	unordered_map<string, string> es_type_map;

	// Set of text fields (need .keyword for exact matching).
	unordered_set<string> text_fields;

	// Sample size for array detection (0 = disabled, default = 100).
	int64_t sample_size = 100;
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

	// Projected column indices (from projection pushdown).
	vector<idx_t> projected_columns;

	// Column names for projected columns (for reading from _source).
	vector<string> projected_field_paths;
	vector<string> projected_es_types;
	vector<LogicalType> projected_types;

	// The final query sent to Elasticsearch (with filters merged).
	std::string final_query;

	ElasticsearchQueryGlobalState() : finished(false), current_row(0), current_hit_idx(0), max_rows(-1) {
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
static std::string BuildFinalQuery(const ElasticsearchQueryBindData &bind_data, const TableFilterSet *filters,
                                   const vector<idx_t> &column_ids, int64_t limit) {
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	yyjson_mut_val *query_clause = nullptr;
	yyjson_mut_val *base_query_clause = nullptr;

	// Parse base query if provided (the query parameter is the query clause itself now).
	if (!bind_data.base_query.empty()) {
		yyjson_doc *base_doc = yyjson_read(bind_data.base_query.c_str(), bind_data.base_query.size(), 0);
		if (base_doc) {
			yyjson_val *base_root = yyjson_doc_get_root(base_doc);
			base_query_clause = yyjson_val_mut_copy(doc, base_root);
			yyjson_doc_free(base_doc);
		}
	}

	// Translate pushed filters to Elasticsearch query DSL.
	yyjson_mut_val *filter_clause = nullptr;
	if (filters && !filters->filters.empty()) {
		// Build column names vector for the filter translator.
		// Important: Filter indices in TableFilterSet are relative to column_ids (the projected columns)
		// and not the original bind schema, we need to map them correctly.
		//
		// column_ids contains indices into the bind schema: [_id (0), ...fields... (1-N), optionally _unmapped_ (N+1)]
		// Filter indices are positions within column_ids.
		vector<string> filter_column_names;
		for (idx_t col_id : column_ids) {
			if (col_id == 0) {
				filter_column_names.push_back("_id");
			} else if (col_id <= bind_data.all_column_names.size()) {
				// regular field column (col_id 1 maps to all_column_names[0], etc.)
				const string &name = bind_data.all_column_names[col_id - 1];
				filter_column_names.push_back(name);
			} else {
				// _unmapped_ column
				filter_column_names.push_back("_unmapped_");
			}
		}

		filter_clause = ElasticsearchFilterTranslator::TranslateFilters(doc, *filters, filter_column_names,
		                                                                bind_data.es_type_map, bind_data.text_fields);
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
	// We need to request only the field paths for projected columns.
	// Important: If _unmapped_ column is projected, we need the full _source to detect unmapped fields.
	bool needs_full_source = false;
	for (idx_t col_id : column_ids) {
		// _unmapped_ column is always at position field_paths.size() + 1 (after _id and all fields).
		if (col_id > bind_data.field_paths.size()) {
			needs_full_source = true;
			break;
		}
	}

	if (!column_ids.empty() && !needs_full_source) {
		vector<string> source_fields;
		for (idx_t col_id : column_ids) {
			// skip _id (col 0)
			if (col_id == 0) {
				continue; // _id is always returned by Elasticsearch
			}
			idx_t field_idx = col_id - 1; // adjust for _id column
			if (field_idx < bind_data.field_paths.size()) {
				source_fields.push_back(bind_data.field_paths[field_idx]);
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

	// Add size/limit if specified.
	if (limit > 0) {
		yyjson_mut_obj_add_int(doc, root, "size", limit);
	}

	// Serialize the query.
	char *json_str = yyjson_mut_write(doc, 0, nullptr);
	std::string result;
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

	// Parse arguments.
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

	// Create client to fetch mapping.
	ElasticsearchClient client(bind_data->config, bind_data->logger);
	auto mapping_response = client.GetMapping(bind_data->index);

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
	MergeMappingsFromIndices(root, bind_data->all_column_names, bind_data->all_column_types, bind_data->field_paths,
	                         bind_data->es_types, bind_data->all_mapped_paths);

	// Collect all path types including nested paths.
	// This is needed for proper filter pushdown on nested struct fields.
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
			}
		}
	}

	yyjson_doc_free(doc);

	// Build Elasticsearch type map and identify text fields.
	// Include both top-level columns and all nested paths.
	for (size_t i = 0; i < bind_data->all_column_names.size(); i++) {
		const string &col_name = bind_data->all_column_names[i];
		const string &es_type = bind_data->es_types[i];
		bind_data->es_type_map[col_name] = es_type;
		if (es_type == "text") {
			bind_data->text_fields.insert(col_name);
		}
	}

	// Add all nested paths to es_type_map and text_fields.
	for (const auto &entry : all_path_types) {
		bind_data->es_type_map[entry.first] = entry.second;
		if (entry.second == "text") {
			bind_data->text_fields.insert(entry.first);
		}
	}

	// Sample documents to detect arrays and unmapped fields.
	std::string sample_query = R"({"query": {"match_all": {}}})";
	if (!bind_data->base_query.empty()) {
		sample_query = R"({"query": )" + bind_data->base_query + "}";
	}
	if (bind_data->sample_size > 0 && !bind_data->field_paths.empty()) {
		SampleResult sample_result =
		    SampleDocuments(client, bind_data->index, sample_query, bind_data->field_paths, bind_data->es_types,
		                    bind_data->all_mapped_paths, bind_data->sample_size);

		// Wrap types in LIST for fields detected as arrays.
		for (size_t i = 0; i < bind_data->field_paths.size(); i++) {
			if (sample_result.array_fields.count(bind_data->field_paths[i])) {
				if (bind_data->all_column_types[i].id() != LogicalTypeId::LIST) {
					bind_data->all_column_types[i] = LogicalType::LIST(bind_data->all_column_types[i]);
				}
			}
		}
	}

	// If no columns found, add a default _source column.
	if (bind_data->all_column_names.empty()) {
		bind_data->all_column_names.push_back("_source");
		bind_data->all_column_types.push_back(LogicalType::VARCHAR);
		bind_data->field_paths.push_back("_source");
		bind_data->es_types.push_back("object");
	}

	// Build output schema: [_id, ...fields..., optionally _unmapped_].
	names.push_back("_id");
	return_types.push_back(LogicalType::VARCHAR);

	for (size_t i = 0; i < bind_data->all_column_names.size(); i++) {
		names.push_back(bind_data->all_column_names[i]);
		return_types.push_back(bind_data->all_column_types[i]);
	}

	// Always add _unmapped_ column to capture fields not in the mapping.
	names.push_back("_unmapped_");
	return_types.push_back(LogicalType::JSON());

	return std::move(bind_data);
}

// Initialize global state with pushdown handling.
static unique_ptr<GlobalTableFunctionState> ElasticsearchQueryInitGlobal(ClientContext &context,
                                                                         TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ElasticsearchQueryBindData>();
	auto state = make_uniq<ElasticsearchQueryGlobalState>();

	// Handle projection pushdown.
	// column_ids contains the indices of columns that are actually needed.
	state->projected_columns = input.column_ids;

	// Build projected field info.
	// Column layout: [_id (0), ...fields... (1 to N), optionally _unmapped_ (N+1 if present)].
	for (idx_t col_id : input.column_ids) {
		if (col_id == 0) {
			// _id column
			state->projected_field_paths.push_back("_id");
			state->projected_es_types.push_back("");
			state->projected_types.push_back(LogicalType::VARCHAR);
		} else if (col_id <= bind_data.field_paths.size()) {
			// regular field column
			idx_t field_idx = col_id - 1;
			state->projected_field_paths.push_back(bind_data.field_paths[field_idx]);
			state->projected_es_types.push_back(bind_data.es_types[field_idx]);
			state->projected_types.push_back(bind_data.all_column_types[field_idx]);
		} else {
			// _unmapped_ column
			state->projected_field_paths.push_back("_unmapped_");
			state->projected_es_types.push_back("");
			state->projected_types.push_back(LogicalType::JSON());
		}
	}

	// Determine limit from MaxRows() if available.
	// Note: DuckDB doesn't always populate this, so we might not have a limit.
	state->max_rows = -1;

	// Build the final query with pushdown.
	state->final_query = BuildFinalQuery(bind_data, input.filters.get(), input.column_ids, state->max_rows);

	// Create client.
	state->client = make_uniq<ElasticsearchClient>(bind_data.config, bind_data.logger);

	// Determine batch size.
	int64_t batch_size = 1000;
	if (state->max_rows > 0 && state->max_rows < batch_size) {
		batch_size = state->max_rows;
	}

	// Start scroll search.
	auto response = state->client->ScrollSearch(bind_data.index, state->final_query, "5m", batch_size);

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

			auto response = state.client->ScrollNext(state.scroll_id, "5m");
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

		// Process current hit.
		yyjson_val *hit = state.hits[state.current_hit_idx];
		yyjson_val *source = yyjson_obj_get(hit, "_source");
		yyjson_val *id_val = yyjson_obj_get(hit, "_id");

		// Process each projected column.
		for (idx_t out_col = 0; out_col < state.projected_columns.size(); out_col++) {
			idx_t col_id = state.projected_columns[out_col];
			const std::string &field_path = state.projected_field_paths[out_col];
			const std::string &es_type = state.projected_es_types[out_col];
			const LogicalType &col_type = state.projected_types[out_col];

			if (col_id == 0) {
				// _id column
				if (id_val && yyjson_is_str(id_val)) {
					auto str_val = StringVector::AddString(output.data[out_col], yyjson_get_str(id_val));
					FlatVector::GetData<string_t>(output.data[out_col])[output_idx] = str_val;
				} else {
					FlatVector::SetNull(output.data[out_col], output_idx, true);
				}
			} else if (field_path == "_unmapped_") {
				// _unmapped_ column
				std::string unmapped_json = CollectUnmappedFields(source, bind_data.all_mapped_paths);
				if (unmapped_json.empty()) {
					FlatVector::SetNull(output.data[out_col], output_idx, true);
				} else {
					auto str_val = StringVector::AddString(output.data[out_col], unmapped_json);
					FlatVector::GetData<string_t>(output.data[out_col])[output_idx] = str_val;
				}
			} else {
				// regular field
				yyjson_val *val = nullptr;
				if (field_path == "_source") {
					val = source;
				} else {
					val = GetValueByPath(source, field_path);
				}
				SetValueFromJson(val, output.data[out_col], output_idx, col_type, es_type);
			}
		}

		output_idx++;
		state.current_hit_idx++;
		state.current_row++;
	}

	output.SetCardinality(output_idx);
}

void RegisterElasticsearchQueryFunction(ExtensionLoader &loader) {
	TableFunction elasticsearch_query("elasticsearch_query", {}, ElasticsearchQueryScan, ElasticsearchQueryBind,
	                                  ElasticsearchQueryInitGlobal);

	// Enable pushdown.
	elasticsearch_query.projection_pushdown = true;
	elasticsearch_query.filter_pushdown = true;

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
