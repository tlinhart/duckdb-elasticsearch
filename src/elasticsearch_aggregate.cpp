#include "elasticsearch_aggregate.hpp"
#include "elasticsearch_common.hpp"
#include "elasticsearch_filter_pushdown.hpp"
#include "elasticsearch_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/client_config.hpp"
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson;

// Bind data for the elasticsearch_aggregate function.
// Contains connection config, schema, base query and all aggregate-specific parameters.
struct ElasticsearchAggregateBindData : public TableFunctionData {
	ElasticsearchConfig config;
	std::string index;
	std::string base_query; // user-provided query (optional, merged with filters)

	// Logger for HTTP request logging, captured from ClientContext during bind.
	shared_ptr<Logger> logger;

	// Resolved Elasticsearch schema containing all mapping and sampling information.
	// Produced by ResolveElasticsearchSchema() and consumed by query building and filter pushdown.
	ElasticsearchSchema schema;

	// Sample size for array detection (0 = disabled).
	// Populated from elasticsearch_sample_size setting, overridable by named parameter.
	int64_t sample_size;

	// The raw aggregation JSON string from the 'aggs' named parameter.
	std::string aggs;

	// Top-level aggregation names extracted from the 'aggs' JSON.
	// These become the output column names (each typed as VARIANT).
	vector<string> aggregation_names;
};

// Global state for the aggregate scan.
struct ElasticsearchAggregateGlobalState : public GlobalTableFunctionState {
	// Parsed aggregation results from Elasticsearch response.
	// Each entry corresponds to a top-level aggregation name and contains the raw JSON subtree
	// as a VariantValue for output.
	vector<VariantValue> aggregation_values;

	// Whether the single result row has been emitted.
	bool finished = false;

	// The parsed response document (owned, must be freed).
	yyjson_doc *response_doc = nullptr;

	~ElasticsearchAggregateGlobalState() {
		if (response_doc) {
			yyjson_doc_free(response_doc);
		}
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Return virtual columns for elasticsearch_aggregate.
// Virtual columns are excluded from SELECT * but can be referenced in WHERE clauses for filter pushdown.
// Layout: VIRTUAL_COLUMN_START + 0 = _id, VIRTUAL_COLUMN_START + 1..N = index fields, VIRTUAL_COLUMN_START + N+1 =
// _unmapped_
static virtual_column_map_t ElasticsearchAggregateGetVirtualColumns(ClientContext &context,
                                                                    optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ElasticsearchAggregateBindData>();
	virtual_column_map_t result;

	// _id at VIRTUAL_COLUMN_START
	result.insert({VIRTUAL_COLUMN_START, {"_id", LogicalType::VARCHAR}});

	// Index fields at VIRTUAL_COLUMN_START + 1 + i
	for (idx_t i = 0; i < bind_data.schema.column_names.size(); i++) {
		result.insert(
		    {VIRTUAL_COLUMN_START + 1 + i, {bind_data.schema.column_names[i], bind_data.schema.column_types[i]}});
	}

	// _unmapped_ at the end
	result.insert(
	    {VIRTUAL_COLUMN_START + 1 + bind_data.schema.column_names.size(), {"_unmapped_", LogicalType::VARIANT()}});

	return result;
}

// Bind function for elasticsearch_aggregate.
static unique_ptr<FunctionData> ElasticsearchAggregateBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<ElasticsearchAggregateBindData>();

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
		throw InvalidInputException("elasticsearch_aggregate requires 'host' parameter");
	}
	if (bind_data->index.empty()) {
		throw InvalidInputException("elasticsearch_aggregate requires 'index' parameter");
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

	// Parse the aggregate-specific 'aggs' parameter.
	bool has_aggs = false;
	for (auto &kv : input.named_parameters) {
		if (kv.first == "aggs") {
			bind_data->aggs = StringValue::Get(kv.second);
			has_aggs = true;
		}
	}

	if (!has_aggs || bind_data->aggs.empty()) {
		throw InvalidInputException("elasticsearch_aggregate requires 'aggs' parameter");
	}

	// Parse and validate the aggs JSON, extract top-level aggregation names.
	yyjson_doc *aggs_doc = yyjson_read(bind_data->aggs.c_str(), bind_data->aggs.size(), 0);
	if (!aggs_doc) {
		throw InvalidInputException("elasticsearch_aggregate: 'aggs' parameter contains invalid JSON");
	}

	yyjson_val *aggs_root = yyjson_doc_get_root(aggs_doc);
	if (!aggs_root || !yyjson_is_obj(aggs_root)) {
		yyjson_doc_free(aggs_doc);
		throw InvalidInputException("elasticsearch_aggregate: 'aggs' parameter must be a JSON object");
	}

	// Extract top-level keys as aggregation names.
	yyjson_obj_iter iter;
	yyjson_obj_iter_init(aggs_root, &iter);
	yyjson_val *key;
	while ((key = yyjson_obj_iter_next(&iter))) {
		bind_data->aggregation_names.push_back(yyjson_get_str(key));
	}
	yyjson_doc_free(aggs_doc);

	if (bind_data->aggregation_names.empty()) {
		throw InvalidInputException("elasticsearch_aggregate: 'aggs' parameter must contain at least one aggregation");
	}

	// Build output schema: one VARIANT column per top-level aggregation.
	for (const auto &agg_name : bind_data->aggregation_names) {
		names.push_back(agg_name);
		return_types.push_back(LogicalType::VARIANT());
	}

	return std::move(bind_data);
}

// Build the Elasticsearch query for aggregation: merges base query + pushed filters + aggs, size=0.
static std::string BuildElasticsearchQuery(const ElasticsearchAggregateBindData &bind_data,
                                           const TableFilterSet *filters, const vector<idx_t> &column_ids) {
	std::string result;

	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	yyjson_mut_val *query_clause = nullptr;
	yyjson_mut_val *base_query_clause = nullptr;

	// Parse base query if provided.
	if (!bind_data.base_query.empty()) {
		yyjson_doc *base_doc = yyjson_read(bind_data.base_query.c_str(), bind_data.base_query.size(), 0);
		if (base_doc) {
			yyjson_val *base_root = yyjson_doc_get_root(base_doc);
			base_query_clause = yyjson_val_mut_copy(doc, base_root);
			yyjson_doc_free(base_doc);
		}
	}

	// Translate pushed filters to Elasticsearch Query DSL.
	yyjson_mut_val *filter_clause = nullptr;
	if (filters && filters->HasFilters()) {
		// Build column names for virtual columns.
		vector<string> filter_column_names;
		for (idx_t col_id : column_ids) {
			if (col_id >= VIRTUAL_COLUMN_START) {
				idx_t virtual_offset = col_id - VIRTUAL_COLUMN_START;
				if (virtual_offset == 0) {
					filter_column_names.push_back("_id");
				} else if (virtual_offset <= bind_data.schema.column_names.size()) {
					filter_column_names.push_back(bind_data.schema.column_names[virtual_offset - 1]);
				} else {
					filter_column_names.push_back("_unmapped_");
				}
			} else {
				// Regular aggregation output column (not an index field).
				// These are not filterable, push a placeholder.
				filter_column_names.push_back("");
			}
		}

		FilterTranslationResult translation_result =
		    TranslateFilters(doc, *filters, filter_column_names, bind_data.schema);
		filter_clause = translation_result.es_query;
	}

	// Merge base query and filter clause.
	if (base_query_clause && filter_clause) {
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
		query_clause = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, query_clause, "match_all", yyjson_mut_obj(doc));
	}

	yyjson_mut_obj_add_val(doc, root, "query", query_clause);

	// Add aggregations.
	yyjson_doc *aggs_doc = yyjson_read(bind_data.aggs.c_str(), bind_data.aggs.size(), 0);
	if (aggs_doc) {
		yyjson_val *aggs_root = yyjson_doc_get_root(aggs_doc);
		yyjson_mut_val *aggs_mut = yyjson_val_mut_copy(doc, aggs_root);
		yyjson_mut_obj_add_val(doc, root, "aggs", aggs_mut);
		yyjson_doc_free(aggs_doc);
	}

	// No hits needed, only aggregation results.
	yyjson_mut_obj_add_int(doc, root, "size", 0);

	// Serialize.
	char *json_str = yyjson_mut_write(doc, 0, nullptr);
	if (json_str) {
		result = json_str;
		free(json_str);
	}

	yyjson_mut_doc_free(doc);
	return result;
}

// Initialize global state for elasticsearch_aggregate.
static unique_ptr<GlobalTableFunctionState> ElasticsearchAggregateInitGlobal(ClientContext &context,
                                                                             TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ElasticsearchAggregateBindData>();
	auto state = make_uniq<ElasticsearchAggregateGlobalState>();

	// Check that no virtual columns are explicitly projected (i.e. in SELECT list).
	// Virtual columns are only for WHERE clause filtering, not for output.
	// When filter_prune is active, column_ids includes both output and filter-only columns,
	// while projection_ids contains indices into column_ids for output columns only.
	// We must only check output columns - virtual columns used only in WHERE are expected
	// in column_ids but should not trigger an error.
	vector<idx_t> output_col_ids;
	if (!input.projection_ids.empty()) {
		for (idx_t proj_idx : input.projection_ids) {
			output_col_ids.push_back(input.column_ids[proj_idx]);
		}
	} else {
		output_col_ids = input.column_ids;
	}
	for (idx_t col_id : output_col_ids) {
		if (col_id >= VIRTUAL_COLUMN_START) {
			// Map virtual column ID back to field name for error message.
			idx_t virtual_offset = col_id - VIRTUAL_COLUMN_START;
			string col_name;
			if (virtual_offset == 0) {
				col_name = "_id";
			} else if (virtual_offset <= bind_data.schema.column_names.size()) {
				col_name = bind_data.schema.column_names[virtual_offset - 1];
			} else {
				col_name = "_unmapped_";
			}
			throw InvalidInputException("elasticsearch_aggregate does not support selecting index field '%s'. "
			                            "Index fields can only be used in WHERE clauses for filter pushdown. "
			                            "Only aggregation result columns are available in SELECT.",
			                            col_name);
		}
	}

	// Build the aggregation query with pushdown.
	std::string query = BuildElasticsearchQuery(bind_data, input.filters.get(), input.column_ids);

	// Execute single search request.
	auto client = make_uniq<ElasticsearchClient>(bind_data.config, bind_data.logger);
	auto response = client->Search(bind_data.index, query, 0);

	if (!response.success) {
		throw IOException("Elasticsearch aggregation failed: " + response.error_message);
	}

	// Parse response.
	state->response_doc = yyjson_read(response.body.c_str(), response.body.size(), 0);
	if (!state->response_doc) {
		throw IOException("Failed to parse Elasticsearch aggregation response");
	}

	yyjson_val *root = yyjson_doc_get_root(state->response_doc);
	yyjson_val *aggregations = yyjson_obj_get(root, "aggregations");
	if (!aggregations || !yyjson_is_obj(aggregations)) {
		throw IOException("Elasticsearch response does not contain aggregation results");
	}

	// Extract aggregation results as VariantValues, but only for projected output columns.
	// When projection pushdown is active, the output DataChunk has fewer columns than the
	// total number of aggregations. We must only populate values for output columns to avoid
	// writing out-of-bounds in the scan function.
	for (idx_t col_id : output_col_ids) {
		const auto &agg_name = bind_data.aggregation_names[col_id];
		yyjson_val *agg_val = yyjson_obj_get(aggregations, agg_name.c_str());
		if (agg_val) {
			state->aggregation_values.push_back(ConvertYyjsonToVariantValue(agg_val));
		} else {
			// Aggregation name not found in response, emit null.
			state->aggregation_values.push_back(VariantValue(Value()));
		}
	}

	return std::move(state);
}

// Scan function for elasticsearch_aggregate: emits a single row.
static void ElasticsearchAggregateScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<ElasticsearchAggregateGlobalState>();

	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	// Emit a single row with aggregation results.
	for (idx_t col = 0; col < state.aggregation_values.size(); col++) {
		vector<VariantValue> values;
		values.push_back(std::move(state.aggregation_values[col]));
		VariantValue::ToVARIANT(values, output.data[col]);
	}

	output.SetCardinality(1);
	state.finished = true;
}

// Thin adapter: casts bind data to ElasticsearchAggregateBindData, creates ElasticsearchPushdownContext,
// and delegates to the shared PushdownComplexFilters implementation in elasticsearch_filter_pushdown.cpp.
static void ElasticsearchAggregatePushdownComplexFilter(ClientContext &context, LogicalGet &get,
                                                        FunctionData *bind_data_p,
                                                        vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<ElasticsearchAggregateBindData>();
	ElasticsearchPushdownContext pushdown_ctx {bind_data.schema};
	PushdownComplexFilters(context, get, pushdown_ctx, filters);
}

void RegisterElasticsearchAggregateFunction(ExtensionLoader &loader) {
	TableFunction elasticsearch_aggregate("elasticsearch_aggregate", {}, ElasticsearchAggregateScan,
	                                      ElasticsearchAggregateBind, ElasticsearchAggregateInitGlobal);

	// Enable pushdown for virtual columns (WHERE clause filter pushdown on index fields).
	elasticsearch_aggregate.projection_pushdown = true;
	elasticsearch_aggregate.filter_pushdown = true;
	elasticsearch_aggregate.filter_prune = true;
	elasticsearch_aggregate.pushdown_complex_filter = ElasticsearchAggregatePushdownComplexFilter;
	elasticsearch_aggregate.get_virtual_columns = ElasticsearchAggregateGetVirtualColumns;

	// Named parameters.
	elasticsearch_aggregate.named_parameters["host"] = LogicalType::VARCHAR;
	elasticsearch_aggregate.named_parameters["port"] = LogicalType::INTEGER;
	elasticsearch_aggregate.named_parameters["index"] = LogicalType::VARCHAR;
	elasticsearch_aggregate.named_parameters["query"] = LogicalType::VARCHAR;
	elasticsearch_aggregate.named_parameters["username"] = LogicalType::VARCHAR;
	elasticsearch_aggregate.named_parameters["password"] = LogicalType::VARCHAR;
	elasticsearch_aggregate.named_parameters["use_ssl"] = LogicalType::BOOLEAN;
	elasticsearch_aggregate.named_parameters["verify_ssl"] = LogicalType::BOOLEAN;
	elasticsearch_aggregate.named_parameters["timeout"] = LogicalType::INTEGER;
	elasticsearch_aggregate.named_parameters["max_retries"] = LogicalType::INTEGER;
	elasticsearch_aggregate.named_parameters["retry_interval"] = LogicalType::INTEGER;
	elasticsearch_aggregate.named_parameters["retry_backoff_factor"] = LogicalType::DOUBLE;
	elasticsearch_aggregate.named_parameters["sample_size"] = LogicalType::INTEGER;
	elasticsearch_aggregate.named_parameters["aggs"] = LogicalType::VARCHAR;

	loader.RegisterFunction(elasticsearch_aggregate);
}

} // namespace duckdb
