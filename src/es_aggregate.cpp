#include "es_aggregate.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson;

// Forward declarations.
static LogicalType InferTypeFromJson(yyjson_val *val);
static void SetValueFromJson(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type);

// Infer DuckDB type from a JSON value.
static LogicalType InferTypeFromJson(yyjson_val *val) {
	if (!val || yyjson_is_null(val)) {
		return LogicalType::VARCHAR; // default for null
	}

	if (yyjson_is_bool(val)) {
		return LogicalType::BOOLEAN;
	}

	if (yyjson_is_int(val) || yyjson_is_sint(val)) {
		return LogicalType::BIGINT;
	}

	if (yyjson_is_uint(val)) {
		return LogicalType::UBIGINT;
	}

	if (yyjson_is_real(val)) {
		return LogicalType::DOUBLE;
	}

	if (yyjson_is_str(val)) {
		return LogicalType::VARCHAR;
	}

	if (yyjson_is_arr(val)) {
		// Infer element type from first non-null element.
		size_t idx, max;
		yyjson_val *elem;
		LogicalType elem_type = LogicalType::VARCHAR; // default

		yyjson_arr_foreach(val, idx, max, elem) {
			if (!yyjson_is_null(elem)) {
				elem_type = InferTypeFromJson(elem);
				break;
			}
		}
		return LogicalType::LIST(elem_type);
	}

	if (yyjson_is_obj(val)) {
		child_list_t<LogicalType> children;

		yyjson_obj_iter iter;
		yyjson_obj_iter_init(val, &iter);
		yyjson_val *key;

		while ((key = yyjson_obj_iter_next(&iter))) {
			const char *field_name = yyjson_get_str(key);
			yyjson_val *field_val = yyjson_obj_iter_get_val(key);

			LogicalType child_type = InferTypeFromJson(field_val);
			children.push_back(make_pair(std::string(field_name), child_type));
		}

		if (children.empty()) {
			return LogicalType::VARCHAR; // empty object -> VARCHAR
		}

		return LogicalType::STRUCT(children);
	}

	return LogicalType::VARCHAR; // fallback
}

// Set a STRUCT value from JSON object.
static void SetStructValueFromJson(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type) {
	if (!val || yyjson_is_null(val)) {
		FlatVector::SetNull(result, row_idx, true);
		return;
	}

	if (!yyjson_is_obj(val)) {
		FlatVector::SetNull(result, row_idx, true);
		return;
	}

	auto &child_entries = StructVector::GetEntries(result);
	auto &child_types = StructType::GetChildTypes(type);

	for (idx_t i = 0; i < child_types.size(); i++) {
		const std::string &child_name = child_types[i].first;
		const LogicalType &child_type = child_types[i].second;

		yyjson_val *child_val = yyjson_obj_get(val, child_name.c_str());
		SetValueFromJson(child_val, *child_entries[i], row_idx, child_type);
	}
}

// Set a LIST value from JSON array.
static void SetListValueFromJson(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type) {
	if (!val || yyjson_is_null(val)) {
		FlatVector::SetNull(result, row_idx, true);
		return;
	}

	if (!yyjson_is_arr(val)) {
		FlatVector::SetNull(result, row_idx, true);
		return;
	}

	auto &list_entry = ListVector::GetEntry(result);
	auto list_size = ListVector::GetListSize(result);
	auto arr_size = yyjson_arr_size(val);

	// Get element type.
	auto &elem_type = ListType::GetChildType(type);

	// Reserve space.
	ListVector::Reserve(result, list_size + arr_size);

	// Set list entry data.
	auto list_data = FlatVector::GetData<list_entry_t>(result);
	list_data[row_idx].offset = list_size;
	list_data[row_idx].length = arr_size;

	// Add elements.
	size_t idx, max;
	yyjson_val *elem;
	idx_t elem_idx = list_size;

	yyjson_arr_foreach(val, idx, max, elem) {
		SetValueFromJson(elem, list_entry, elem_idx, elem_type);
		elem_idx++;
	}

	ListVector::SetListSize(result, list_size + arr_size);
}

// Set a value in a vector from a JSON value.
static void SetValueFromJson(yyjson_val *val, Vector &result, idx_t row_idx, const LogicalType &type) {
	if (!val || yyjson_is_null(val)) {
		FlatVector::SetNull(result, row_idx, true);
		return;
	}

	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		FlatVector::GetData<bool>(result)[row_idx] = yyjson_get_bool(val);
		break;

	case LogicalTypeId::TINYINT:
		FlatVector::GetData<int8_t>(result)[row_idx] = static_cast<int8_t>(yyjson_get_sint(val));
		break;

	case LogicalTypeId::SMALLINT:
		FlatVector::GetData<int16_t>(result)[row_idx] = static_cast<int16_t>(yyjson_get_sint(val));
		break;

	case LogicalTypeId::INTEGER:
		FlatVector::GetData<int32_t>(result)[row_idx] = static_cast<int32_t>(yyjson_get_sint(val));
		break;

	case LogicalTypeId::BIGINT:
		FlatVector::GetData<int64_t>(result)[row_idx] = yyjson_get_sint(val);
		break;

	case LogicalTypeId::UTINYINT:
		FlatVector::GetData<uint8_t>(result)[row_idx] = static_cast<uint8_t>(yyjson_get_uint(val));
		break;

	case LogicalTypeId::USMALLINT:
		FlatVector::GetData<uint16_t>(result)[row_idx] = static_cast<uint16_t>(yyjson_get_uint(val));
		break;

	case LogicalTypeId::UINTEGER:
		FlatVector::GetData<uint32_t>(result)[row_idx] = static_cast<uint32_t>(yyjson_get_uint(val));
		break;

	case LogicalTypeId::UBIGINT:
		FlatVector::GetData<uint64_t>(result)[row_idx] = yyjson_get_uint(val);
		break;

	case LogicalTypeId::FLOAT:
		if (yyjson_is_real(val)) {
			FlatVector::GetData<float>(result)[row_idx] = static_cast<float>(yyjson_get_real(val));
		} else {
			FlatVector::GetData<float>(result)[row_idx] = static_cast<float>(yyjson_get_sint(val));
		}
		break;

	case LogicalTypeId::DOUBLE:
		if (yyjson_is_real(val)) {
			FlatVector::GetData<double>(result)[row_idx] = yyjson_get_real(val);
		} else {
			FlatVector::GetData<double>(result)[row_idx] = static_cast<double>(yyjson_get_sint(val));
		}
		break;

	case LogicalTypeId::VARCHAR: {
		std::string str_val;
		if (yyjson_is_str(val)) {
			str_val = yyjson_get_str(val);
		} else {
			// Convert non-string values to JSON string.
			char *json_str = yyjson_val_write(val, 0, nullptr);
			if (json_str) {
				str_val = json_str;
				free(json_str);
			}
		}
		auto sv = StringVector::AddString(result, str_val);
		FlatVector::GetData<string_t>(result)[row_idx] = sv;
		break;
	}

	case LogicalTypeId::LIST:
		SetListValueFromJson(val, result, row_idx, type);
		break;

	case LogicalTypeId::STRUCT:
		SetStructValueFromJson(val, result, row_idx, type);
		break;

	default: {
		// Fallback: convert to string.
		char *json_str = yyjson_val_write(val, 0, nullptr);
		if (json_str) {
			auto sv = StringVector::AddString(result, json_str);
			FlatVector::GetData<string_t>(result)[row_idx] = sv;
			free(json_str);
		} else {
			FlatVector::SetNull(result, row_idx, true);
		}
		break;
	}
	}
}

// Bind data for the es_aggregate function.
struct ElasticsearchAggregateBindData : public TableFunctionData {
	ElasticsearchConfig config;
	std::string index;
	std::string query;

	// Store the aggregation result and its type (determined at bind time).
	std::string result_json;
	LogicalType result_type;
};

// Global state for aggregation results.
struct ElasticsearchAggregateGlobalState : public GlobalTableFunctionState {
	bool finished;

	ElasticsearchAggregateGlobalState() : finished(false) {
	}
};

// Bind function - executes the query to determine schema.
static unique_ptr<FunctionData> ElasticsearchAggregateBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<ElasticsearchAggregateBindData>();

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
		}
	}

	// Validate required parameters.
	if (bind_data->config.host.empty()) {
		throw InvalidInputException("es_aggregate requires 'host' parameter");
	}
	if (bind_data->index.empty()) {
		throw InvalidInputException("es_aggregate requires 'index' parameter");
	}
	if (bind_data->query.empty()) {
		throw InvalidInputException("es_aggregate requires 'query' parameter with aggregation definition");
	}

	// Execute the aggregation query during bind to determine schema.
	ElasticsearchClient client(bind_data->config);
	auto response = client.Aggregate(bind_data->index, bind_data->query);

	if (!response.success) {
		throw IOException("Elasticsearch aggregation failed: " + response.error_message);
	}

	// Parse the response and extract aggregations.
	yyjson_doc *doc = yyjson_read(response.body.c_str(), response.body.size(), 0);
	if (!doc) {
		throw IOException("Failed to parse Elasticsearch response");
	}

	yyjson_val *root = yyjson_doc_get_root(doc);
	yyjson_val *aggs = yyjson_obj_get(root, "aggregations");

	if (aggs) {
		// Store the JSON for later.
		char *agg_json = yyjson_val_write(aggs, 0, nullptr);
		if (agg_json) {
			bind_data->result_json = agg_json;
			free(agg_json);
		}

		// Infer the type from the aggregations object.
		bind_data->result_type = InferTypeFromJson(aggs);
	} else {
		// No aggregations key - store full response as VARCHAR.
		bind_data->result_json = response.body;
		bind_data->result_type = LogicalType::VARCHAR;
	}

	yyjson_doc_free(doc);

	// Set return type.
	names.push_back("aggregations");
	return_types.push_back(bind_data->result_type);

	return std::move(bind_data);
}

// Initialize global state (query already executed in bind).
static unique_ptr<GlobalTableFunctionState> ElasticsearchAggregateInitGlobal(ClientContext &context,
                                                                             TableFunctionInitInput &input) {
	return make_uniq<ElasticsearchAggregateGlobalState>();
}

// Scan function.
static void ElasticsearchAggregateScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<ElasticsearchAggregateBindData>();
	auto &state = data.global_state->Cast<ElasticsearchAggregateGlobalState>();

	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	// Parse the stored JSON result.
	yyjson_doc *doc = yyjson_read(bind_data.result_json.c_str(), bind_data.result_json.size(), 0);
	if (!doc) {
		output.SetCardinality(0);
		state.finished = true;
		return;
	}

	yyjson_val *root = yyjson_doc_get_root(doc);

	// Set the value using the inferred type.
	SetValueFromJson(root, output.data[0], 0, bind_data.result_type);

	yyjson_doc_free(doc);

	output.SetCardinality(1);
	state.finished = true;
}

void RegisterElasticsearchAggregateFunction(ExtensionLoader &loader) {
	TableFunction es_aggregate("es_aggregate", {}, ElasticsearchAggregateScan, ElasticsearchAggregateBind,
	                           ElasticsearchAggregateInitGlobal);

	// Named parameters.
	es_aggregate.named_parameters["host"] = LogicalType::VARCHAR;
	es_aggregate.named_parameters["port"] = LogicalType::INTEGER;
	es_aggregate.named_parameters["index"] = LogicalType::VARCHAR;
	es_aggregate.named_parameters["query"] = LogicalType::VARCHAR;
	es_aggregate.named_parameters["username"] = LogicalType::VARCHAR;
	es_aggregate.named_parameters["password"] = LogicalType::VARCHAR;
	es_aggregate.named_parameters["use_ssl"] = LogicalType::BOOLEAN;
	es_aggregate.named_parameters["verify_ssl"] = LogicalType::BOOLEAN;
	es_aggregate.named_parameters["timeout"] = LogicalType::INTEGER;
	es_aggregate.named_parameters["max_retries"] = LogicalType::INTEGER;
	es_aggregate.named_parameters["retry_interval"] = LogicalType::INTEGER;
	es_aggregate.named_parameters["retry_backoff_factor"] = LogicalType::DOUBLE;

	loader.RegisterFunction(es_aggregate);
}

} // namespace duckdb
