#include "es_search.hpp"
#include "es_common.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "yyjson.hpp"

#include <algorithm>
#include <functional>
#include <map>
#include <set>

namespace duckdb {

using namespace duckdb_yyjson;

// Bind data for the es_search function.
struct ElasticsearchSearchBindData : public TableFunctionData {
	ElasticsearchConfig config;
	std::string index;
	std::string query;

	// Schema information.
	vector<string> column_names;
	vector<LogicalType> column_types;

	// For storing the Elasticsearch field paths (may differ from column names for nested fields).
	vector<string> field_paths;

	// For storing ALL mapped field paths including nested (for unmapped detection).
	std::set<string> all_mapped_paths;

	// Store Elasticsearch types for special handling (geo types).
	vector<string> es_types;

	// Limit from query (if specified).
	int64_t limit = -1;

	// Sample size for array detection (0 = disabled, default = 100).
	int64_t sample_size = 100;
};

// Global state for scanning.
struct ElasticsearchSearchGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<ElasticsearchClient> client;
	std::string scroll_id;
	bool finished;
	idx_t current_row;

	// Parsed documents from current scroll batch.
	vector<yyjson_doc *> docs;
	vector<yyjson_val *> hits;
	idx_t current_hit_idx;

	// Total rows to return (from size/limit).
	int64_t max_rows;

	ElasticsearchSearchGlobalState() : finished(false), current_row(0), current_hit_idx(0), max_rows(-1) {
	}

	~ElasticsearchSearchGlobalState() {
		for (auto doc : docs) {
			if (doc)
				yyjson_doc_free(doc);
		}
		// Clear scroll if active.
		if (client && !scroll_id.empty()) {
			client->ClearScroll(scroll_id);
		}
	}
};

// Bind function - called to determine output schema.
static unique_ptr<FunctionData> ElasticsearchSearchBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<ElasticsearchSearchBindData>();

	// Parse arguments (config has defaults from ElasticsearchConfig struct definition).
	for (auto &kv : input.named_parameters) {
		if (kv.first == "host") {
			bind_data->config.host = StringValue::Get(kv.second);
		} else if (kv.first == "port") {
			bind_data->config.port = IntegerValue::Get(kv.second);
		} else if (kv.first == "index") {
			bind_data->index = StringValue::Get(kv.second);
		} else if (kv.first == "query") {
			bind_data->query = StringValue::Get(kv.second);
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
		throw InvalidInputException("es_search requires 'host' parameter");
	}
	if (bind_data->index.empty()) {
		throw InvalidInputException("es_search requires 'index' parameter");
	}

	if (bind_data->query.empty()) {
		bind_data->query = R"({"query": {"match_all": {}}})";
	}

	// Extract size from query if present.
	bind_data->limit = ExtractSizeFromQuery(bind_data->query);

	// Create client to fetch mapping.
	ElasticsearchClient client(bind_data->config);
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

	// Merge mappings from all matching indices (handles patterns like logs-*).
	// This will throw if incompatible types are found across indices.
	MergeMappingsFromIndices(root, bind_data->column_names, bind_data->column_types, bind_data->field_paths,
	                         bind_data->es_types, bind_data->all_mapped_paths);

	yyjson_doc_free(doc);

	// Sample documents to detect which fields contain arrays.
	// This is needed because Elasticsearch mappings don't distinguish between
	// single values and arrays - a field can contain either at runtime.
	if (bind_data->sample_size > 0 && !bind_data->field_paths.empty()) {
		std::set<std::string> array_fields =
		    DetectArrayFields(client, bind_data->index, bind_data->query, bind_data->field_paths, bind_data->es_types,
		                      bind_data->sample_size);

		// Wrap types in LIST for fields detected as arrays.
		for (size_t i = 0; i < bind_data->field_paths.size(); i++) {
			if (array_fields.count(bind_data->field_paths[i])) {
				// Don't double-wrap if already a LIST (e.g., nested type).
				if (bind_data->column_types[i].id() != LogicalTypeId::LIST) {
					bind_data->column_types[i] = LogicalType::LIST(bind_data->column_types[i]);
				}
			}
		}
	}

	// If no columns found, add a default _source column.
	if (bind_data->column_names.empty()) {
		bind_data->column_names.push_back("_source");
		bind_data->column_types.push_back(LogicalType::VARCHAR);
		bind_data->field_paths.push_back("_source");
		bind_data->es_types.push_back("object");
	}

	// Always add _id column at the beginning.
	names.push_back("_id");
	return_types.push_back(LogicalType::VARCHAR);

	for (size_t i = 0; i < bind_data->column_names.size(); i++) {
		names.push_back(bind_data->column_names[i]);
		return_types.push_back(bind_data->column_types[i]);
	}

	// Always add _unmapped_ column at the end (JSON type for unmapped/dynamic fields).
	names.push_back("_unmapped_");
	return_types.push_back(LogicalType::JSON());

	// Store the types for later use.
	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

// Initialize global state.
static unique_ptr<GlobalTableFunctionState> ElasticsearchSearchInitGlobal(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ElasticsearchSearchBindData>();
	auto state = make_uniq<ElasticsearchSearchGlobalState>();

	// Set max_rows from limit.
	state->max_rows = bind_data.limit;

	// Create client using config directly from bind_data.
	state->client = make_uniq<ElasticsearchClient>(bind_data.config);

	// Determine batch size - use smaller of limit or default batch size.
	int64_t batch_size = 1000;
	if (state->max_rows > 0 && state->max_rows < batch_size) {
		batch_size = state->max_rows;
	}

	// Start scroll search.
	auto response = state->client->ScrollSearch(bind_data.index, bind_data.query, "5m", batch_size);

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
static void ElasticsearchSearchScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<ElasticsearchSearchBindData>();
	auto &state = data.global_state->Cast<ElasticsearchSearchGlobalState>();

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
			// Check if we've hit the limit..
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

		// Set _id column (first column).
		if (id_val && yyjson_is_str(id_val)) {
			auto str_val = StringVector::AddString(output.data[0], yyjson_get_str(id_val));
			FlatVector::GetData<string_t>(output.data[0])[output_idx] = str_val;
		} else {
			FlatVector::SetNull(output.data[0], output_idx, true);
		}

		// Set mapped columns (skip first column _id and last column _unmapped_).
		// Column layout: [_id, ...mapped_fields..., _unmapped_].
		idx_t unmapped_col_idx = output.ColumnCount() - 1;
		for (idx_t col_idx = 1; col_idx < unmapped_col_idx; col_idx++) {
			const std::string &field_path = bind_data.field_paths[col_idx - 1];
			const std::string &es_type = bind_data.es_types[col_idx - 1];

			yyjson_val *val = nullptr;
			if (field_path == "_source") {
				val = source;
			} else {
				val = GetValueByPath(source, field_path);
			}

			SetValueFromJson(val, output.data[col_idx], output_idx, bind_data.column_types[col_idx], es_type);
		}

		// Set _unmapped_ column (last column) - collect fields not in the mapping.
		// Use all_mapped_paths which includes nested paths for accurate detection.
		std::string unmapped_json = CollectUnmappedFields(source, bind_data.all_mapped_paths);

		if (unmapped_json.empty()) {
			FlatVector::SetNull(output.data[unmapped_col_idx], output_idx, true);
		} else {
			auto str_val = StringVector::AddString(output.data[unmapped_col_idx], unmapped_json);
			FlatVector::GetData<string_t>(output.data[unmapped_col_idx])[output_idx] = str_val;
		}

		output_idx++;
		state.current_hit_idx++;
		state.current_row++;
	}

	output.SetCardinality(output_idx);
}

void RegisterElasticsearchSearchFunction(ExtensionLoader &loader) {
	TableFunction es_search("es_search", {}, ElasticsearchSearchScan, ElasticsearchSearchBind,
	                        ElasticsearchSearchInitGlobal);

	// Named parameters.
	es_search.named_parameters["host"] = LogicalType::VARCHAR;
	es_search.named_parameters["port"] = LogicalType::INTEGER;
	es_search.named_parameters["index"] = LogicalType::VARCHAR;
	es_search.named_parameters["query"] = LogicalType::VARCHAR;
	es_search.named_parameters["username"] = LogicalType::VARCHAR;
	es_search.named_parameters["password"] = LogicalType::VARCHAR;
	es_search.named_parameters["use_ssl"] = LogicalType::BOOLEAN;
	es_search.named_parameters["verify_ssl"] = LogicalType::BOOLEAN;
	es_search.named_parameters["timeout"] = LogicalType::INTEGER;
	es_search.named_parameters["max_retries"] = LogicalType::INTEGER;
	es_search.named_parameters["retry_interval"] = LogicalType::INTEGER;
	es_search.named_parameters["retry_backoff_factor"] = LogicalType::DOUBLE;
	es_search.named_parameters["sample_size"] = LogicalType::INTEGER;

	loader.RegisterFunction(es_search);
}

} // namespace duckdb
