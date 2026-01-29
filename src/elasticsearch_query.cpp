#include "elasticsearch_query.hpp"
#include "elasticsearch_common.hpp"
#include "elasticsearch_filter_pushdown.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "yyjson.hpp"

#include <algorithm>
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

	// Set of text fields that have a .keyword subfield (enables filter pushdown).
	unordered_set<string> text_fields_with_keyword;

	// Sample size for array detection (0 = disabled, default = 100).
	int64_t sample_size = 100;

	// Limit pushdown values (set by optimizer extension).
	// -1 means no limit, 0 means no offset.
	int64_t limit = -1;
	int64_t offset = 0;
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

	// Projected column indices (from projection pushdown).
	vector<idx_t> projected_columns;

	// Column names for projected columns (for reading from _source).
	vector<string> projected_field_paths;
	vector<string> projected_es_types;
	vector<LogicalType> projected_types;

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
static std::string BuildFinalQuery(const ElasticsearchQueryBindData &bind_data, const TableFilterSet *filters,
                                   const vector<idx_t> &column_ids, const vector<idx_t> &projection_ids) {
	std::string result;

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

	// Translate pushed filters to Elasticsearch Query DSL.
	// IS NULL / IS NOT NULL filters are now handled through table_filters (added by pushdown_complex_filter).
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

		FilterTranslationResult translation_result =
		    TranslateFilters(doc, *filters, filter_column_names, bind_data.es_type_map, bind_data.text_fields,
		                     bind_data.text_fields_with_keyword);

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
		if (col_id > bind_data.field_paths.size()) {
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
	// Also collect text fields that have a .keyword subfield.
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
				CollectTextFieldsWithKeyword(properties, "", bind_data->text_fields_with_keyword);
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
	// Sampling uses the user-provided query parameter (base_query) if specified, otherwise match_all.
	// This is the best approximation of the actual query because filter pushdown (WHERE clauses)
	// happens after bind time, so the final query with pushed-down filters is not known when
	// sampling occurs. The actual query sent to Elasticsearch might include additional pushed-down
	// filters that are not reflected in the sampling query.
	std::string sampling_query = R"({"query": {"match_all": {}}})";
	if (!bind_data->base_query.empty()) {
		sampling_query = R"({"query": )" + bind_data->base_query + "}";
	}
	if (bind_data->sample_size > 0 && !bind_data->field_paths.empty()) {
		SampleResult sample_result =
		    SampleDocuments(client, bind_data->index, sampling_query, bind_data->field_paths, bind_data->es_types,
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

	// Handle projection pushdown with filter pruning.
	// column_ids contains indices of all columns needed (output + filter-only).
	// projection_ids contains indices into column_ids for output columns only.
	// If projection_ids is empty, all column_ids are output columns (no filter pruning).
	//
	// We build projected_* arrays only for output columns, since:
	// 1. filter-only columns are excluded from _source (Elasticsearch filters server-side)
	// 2. the output DataChunk has projection_ids.size() columns (or column_ids.size() if empty)
	// 3. we write directly to output.data[i] where i corresponds to output column index

	bool has_filter_prune = !input.projection_ids.empty() && input.projection_ids.size() < input.column_ids.size();

	if (has_filter_prune) {
		// Filter pruning is active, only build metadata for output columns.
		// projected_columns will contain the actual column IDs for output columns.
		for (idx_t proj_idx : input.projection_ids) {
			idx_t col_id = input.column_ids[proj_idx];
			state->projected_columns.push_back(col_id);

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
	} else {
		// No filter pruning: all column_ids are output columns.
		state->projected_columns = input.column_ids;

		// Build projected field info for all columns.
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
	state->final_query = BuildFinalQuery(bind_data, input.filters.get(), input.column_ids, input.projection_ids);

	// Create client.
	state->client = make_uniq<ElasticsearchClient>(bind_data.config, bind_data.logger);

	// Determine batch size. For small query limits (up to 5000), fetch all needed rows in one request.
	// For larger limits, keep the default batch size to avoid memory issues with large single requests.
	// Note: When query_limit > 5000, the last batch may overfetch documents. For example, if we need
	// 5010 documents total, we fetch 5 batches of 1000 and one batch of 1000 (instead of 10). This is
	// acceptable given the expected usage pattern of small limits and rare large offsets.
	int64_t batch_size = 1000;
	if (query_limit > 0 && query_limit <= 5000) {
		batch_size = query_limit;
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

// Result of ExtractColumnPath containing column info needed for filter pushdown.
struct ColumnPathInfo {
	idx_t output_col_idx = DConstants::INVALID_INDEX;
	string full_path;
	vector<string> nested_fields;

	bool IsValid() const {
		return output_col_idx != DConstants::INVALID_INDEX;
	}
};

// Extract column path from an expression for filter pushdown.
// Handles direct column references and struct_extract chains for nested object fields.
//
// Examples:
// - BOUND_COLUMN_REF(col=2) -> {2, "name", []}
// - struct_extract(col, 'name') -> {col_idx, "employee.name", ["name"]}
// - struct_extract(struct_extract(col, 'address'), 'city') -> {col_idx, "employee.address.city", ["address", "city"]}
static ColumnPathInfo ExtractColumnPath(const Expression &expr, const ElasticsearchQueryBindData &bind_data,
                                        const vector<ColumnIndex> &column_ids) {
	ColumnPathInfo result;

	// Direct column reference.
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		idx_t output_col_idx = col_ref.binding.column_index;

		if (output_col_idx >= column_ids.size()) {
			return result;
		}

		const ColumnIndex &col_index = column_ids[output_col_idx];
		idx_t bind_col_id = col_index.GetPrimaryIndex();

		// _id column has bind_col_id == 0
		if (bind_col_id == 0) {
			result.output_col_idx = output_col_idx;
			result.full_path = "_id";
			return result;
		}

		if (bind_col_id > bind_data.all_column_names.size()) {
			return result;
		}

		result.output_col_idx = output_col_idx;
		result.full_path = bind_data.all_column_names[bind_col_id - 1];
		return result;
	}

	// struct_extract function for nested fields (e.g. employee.name, employee.address.city)
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();

		if (func_expr.function.name != "struct_extract" || func_expr.children.size() != 2) {
			return result;
		}

		if (func_expr.children[1]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return result;
		}

		auto &field_name_expr = func_expr.children[1]->Cast<BoundConstantExpression>();
		if (field_name_expr.value.type().id() != LogicalTypeId::VARCHAR) {
			return result;
		}

		string field_name = StringValue::Get(field_name_expr.value);

		// Recursively get the parent path.
		ColumnPathInfo parent_result = ExtractColumnPath(*func_expr.children[0], bind_data, column_ids);
		if (!parent_result.IsValid()) {
			return result;
		}

		result.output_col_idx = parent_result.output_col_idx;
		result.full_path = parent_result.full_path + "." + field_name;
		result.nested_fields = std::move(parent_result.nested_fields);
		result.nested_fields.push_back(field_name);
		return result;
	}

	return result;
}

// Wrap a filter in StructFilter for each nesting level.
// For nested_fields = ["address", "city"] wraps as StructFilter("address", StructFilter("city", inner_filter)).
static unique_ptr<TableFilter> WrapInStructFilters(unique_ptr<TableFilter> inner_filter,
                                                   const vector<string> &nested_fields) {
	unique_ptr<TableFilter> result = std::move(inner_filter);
	for (auto it = nested_fields.rbegin(); it != nested_fields.rend(); ++it) {
		result = make_uniq<StructFilter>(0, *it, std::move(result));
	}
	return result;
}

// Pushdown complex filter callback.
// Extracts filters from expressions that DuckDB's FilterCombiner would not (or only partially)
// push down. By handling them here, we can fully push them to Elasticsearch and avoid a redundant
// FILTER node in the query plan.
//
// Handles:
// - IS NULL / IS NOT NULL: FilterCombiner doesn't convert these to TableFilters
// - IN expressions: FilterCombiner wraps non-dense IN filters in OptionalFilter (partial pushdown)
// - LIKE/ILIKE expressions: FilterCombiner converts prefix patterns to range filters (partial pushdown)
// - Comparison expressions: Validates text fields have .keyword subfield (errors thrown at plan time)
static void ElasticsearchPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                               vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<ElasticsearchQueryBindData>();
	const auto &column_ids = get.GetColumnIds();

	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];
		if (!filter) {
			continue;
		}

		// Validate comparison expressions on text fields.
		// DuckDB's FilterCombiner will convert these to ConstantFilter and push them down
		// but TranslateConstantComparison() will throw an error at execution time.
		// By validating here we fail early (even during EXPLAIN).
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto &comp_expr = filter->Cast<BoundComparisonExpression>();

			// Try to extract column path from left side, then right side.
			ColumnPathInfo col_path_info = ExtractColumnPath(*comp_expr.left, bind_data, column_ids);
			if (!col_path_info.IsValid()) {
				col_path_info = ExtractColumnPath(*comp_expr.right, bind_data, column_ids);
			}

			if (col_path_info.IsValid()) {
				const string &col_name = col_path_info.full_path;
				bool is_text_field = bind_data.text_fields.count(col_name) > 0;
				bool has_keyword_subfield = bind_data.text_fields_with_keyword.count(col_name) > 0;

				if (is_text_field && !has_keyword_subfield) {
					throw InvalidInputException(
					    "Cannot filter on text field '%s' because it lacks a .keyword subfield. Options:\n"
					    "  - Add a .keyword subfield to the Elasticsearch mapping\n"
					    "  - Use the 'query' parameter with native Elasticsearch text queries",
					    col_name);
				}
			}
			// Don't consume the filter, let FilterCombiner handle it normally.
			continue;
		}

		// Handle LIKE/ILIKE patterns and optimized string functions (prefix, suffix, contains).
		// DuckDB's optimizer transforms LIKE patterns before filter pushdown:
		// - LikeOptimizationRule: LIKE 'prefix%' -> prefix(), LIKE '%suffix' -> suffix() etc.
		// - FilterCombiner: converts prefix() to range filters and returns PUSHED_DOWN_PARTIALLY
		// By intercepting here, we can use Elasticsearch's native prefix/wildcard queries.
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &func_expr = filter->Cast<BoundFunctionExpression>();
			const auto &func_name = func_expr.function.name;

			if (func_name == "~~" || func_name == "like_escape" || func_name == "~~*" || func_name == "ilike_escape" ||
			    func_name == "prefix" || func_name == "suffix" || func_name == "contains") {
				if (func_expr.children.size() < 2) {
					continue;
				}

				if (func_expr.children[1]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					continue;
				}

				auto &pattern_expr = func_expr.children[1]->Cast<BoundConstantExpression>();
				if (pattern_expr.value.type().id() != LogicalTypeId::VARCHAR) {
					continue;
				}

				ColumnPathInfo col_path_info = ExtractColumnPath(*func_expr.children[0], bind_data, column_ids);
				if (!col_path_info.IsValid()) {
					continue;
				}

				const string &col_name = col_path_info.full_path;
				const ColumnIndex &col_index = column_ids[col_path_info.output_col_idx];
				bool is_text_field = bind_data.text_fields.count(col_name) > 0;
				bool has_keyword_subfield = bind_data.text_fields_with_keyword.count(col_name) > 0;

				// Text fields without .keyword subfield don't support pattern matching correctly.
				if (is_text_field && !has_keyword_subfield) {
					throw InvalidInputException(
					    "Cannot filter on text field '%s' because it lacks a .keyword subfield. Options:\n"
					    "  - Add a .keyword subfield to the Elasticsearch mapping\n"
					    "  - Use the 'query' parameter with native Elasticsearch text queries",
					    col_name);
				}

				unique_ptr<TableFilter> expr_filter = make_uniq<ExpressionFilter>(filter->Copy());
				if (!col_path_info.nested_fields.empty()) {
					expr_filter = WrapInStructFilters(std::move(expr_filter), col_path_info.nested_fields);
				}

				get.table_filters.PushFilter(col_index, std::move(expr_filter));
				filters[i] = nullptr;
				continue;
			}
		}

		// Handle IS NULL, IS NOT NULL and IN expressions.
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
			auto &op_expr = filter->Cast<BoundOperatorExpression>();
			auto expr_type = op_expr.GetExpressionType();

			// IS NULL / IS NOT NULL
			if (expr_type == ExpressionType::OPERATOR_IS_NULL || expr_type == ExpressionType::OPERATOR_IS_NOT_NULL) {
				if (op_expr.children.size() != 1) {
					continue;
				}

				ColumnPathInfo col_path_info = ExtractColumnPath(*op_expr.children[0], bind_data, column_ids);
				if (!col_path_info.IsValid()) {
					continue;
				}

				const ColumnIndex &col_index = column_ids[col_path_info.output_col_idx];

				unique_ptr<TableFilter> null_filter;
				if (expr_type == ExpressionType::OPERATOR_IS_NULL) {
					null_filter = make_uniq<IsNullFilter>();
				} else {
					null_filter = make_uniq<IsNotNullFilter>();
				}

				if (!col_path_info.nested_fields.empty()) {
					null_filter = WrapInStructFilters(std::move(null_filter), col_path_info.nested_fields);
				}

				get.table_filters.PushFilter(col_index, std::move(null_filter));
				filters[i] = nullptr;
				continue;
			}

			// IN expressions
			if (expr_type == ExpressionType::COMPARE_IN) {
				if (op_expr.children.size() < 2) {
					continue;
				}

				ColumnPathInfo col_path_info = ExtractColumnPath(*op_expr.children[0], bind_data, column_ids);
				if (!col_path_info.IsValid()) {
					continue;
				}

				const string &col_name = col_path_info.full_path;
				const ColumnIndex &col_index = column_ids[col_path_info.output_col_idx];

				bool is_text_field = bind_data.text_fields.count(col_name) > 0;
				bool has_keyword_subfield = bind_data.text_fields_with_keyword.count(col_name) > 0;
				if (is_text_field && !has_keyword_subfield) {
					throw InvalidInputException(
					    "Cannot filter on text field '%s' because it lacks a .keyword subfield. Options:\n"
					    "  - Add a .keyword subfield to the Elasticsearch mapping\n"
					    "  - Use the 'query' parameter with native Elasticsearch text queries",
					    col_name);
				}

				// All IN values must be non-null constants.
				vector<Value> in_values;
				bool all_constants = true;
				for (idx_t j = 1; j < op_expr.children.size(); j++) {
					if (op_expr.children[j]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
						all_constants = false;
						break;
					}
					auto &const_expr = op_expr.children[j]->Cast<BoundConstantExpression>();
					if (const_expr.value.IsNull()) {
						all_constants = false;
						break;
					}
					in_values.push_back(const_expr.value);
				}

				if (!all_constants || in_values.empty()) {
					continue;
				}

				unique_ptr<TableFilter> in_filter = make_uniq<InFilter>(std::move(in_values));
				if (!col_path_info.nested_fields.empty()) {
					in_filter = WrapInStructFilters(std::move(in_filter), col_path_info.nested_fields);
				}

				get.table_filters.PushFilter(col_index, std::move(in_filter));
				filters[i] = nullptr;
				continue;
			}
		}
	}

	// Remove processed filters.
	filters.erase(
	    std::remove_if(filters.begin(), filters.end(), [](const unique_ptr<Expression> &e) { return e == nullptr; }),
	    filters.end());
}

void RegisterElasticsearchQueryFunction(ExtensionLoader &loader) {
	TableFunction elasticsearch_query("elasticsearch_query", {}, ElasticsearchQueryScan, ElasticsearchQueryBind,
	                                  ElasticsearchQueryInitGlobal);

	// Enable pushdown.
	elasticsearch_query.projection_pushdown = true;
	elasticsearch_query.filter_pushdown = true;
	elasticsearch_query.filter_prune = true;
	elasticsearch_query.pushdown_complex_filter = ElasticsearchPushdownComplexFilter;

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

// Helper function for the optimizer extension to set limit/offset in bind data.
// Called from elasticsearch_optimizer.cpp after verifying the function name is "elasticsearch_query".
void SetElasticsearchLimitOffset(FunctionData &bind_data, int64_t limit, int64_t offset) {
	auto &es_bind_data = bind_data.Cast<ElasticsearchQueryBindData>();
	es_bind_data.limit = limit;
	es_bind_data.offset = offset;
}

} // namespace duckdb
