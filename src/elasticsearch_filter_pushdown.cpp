#include "elasticsearch_filter_pushdown.hpp"
#include "elasticsearch_common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

using namespace duckdb_yyjson;

// Forward declarations of static helper functions.
static yyjson_mut_val *TranslateFilter(yyjson_mut_doc *doc, const TableFilter &filter, const string &column_name,
                                       const unordered_map<string, string> &es_types,
                                       const unordered_set<string> &text_fields);

static yyjson_mut_val *TranslateConstantComparison(yyjson_mut_doc *doc, const ConstantFilter &filter,
                                                   const string &field_name, bool is_text_field);

static yyjson_mut_val *TranslateConjunctionAnd(yyjson_mut_doc *doc, const ConjunctionAndFilter &filter,
                                               const string &column_name, const unordered_map<string, string> &es_types,
                                               const unordered_set<string> &text_fields);

static yyjson_mut_val *TranslateConjunctionOr(yyjson_mut_doc *doc, const ConjunctionOrFilter &filter,
                                              const string &column_name, const unordered_map<string, string> &es_types,
                                              const unordered_set<string> &text_fields);

static yyjson_mut_val *TranslateInFilter(yyjson_mut_doc *doc, const InFilter &filter, const string &field_name,
                                         bool is_text_field);

static yyjson_mut_val *TranslateExpressionFilter(yyjson_mut_doc *doc, const ExpressionFilter &filter,
                                                 const string &column_name, bool is_text_field);

static yyjson_mut_val *TranslateLikePattern(yyjson_mut_doc *doc, const string &field_name, const string &pattern,
                                            bool is_text_field, bool case_insensitive);

static yyjson_mut_val *TranslateIsNull(yyjson_mut_doc *doc, const string &field_name);

static yyjson_mut_val *TranslateIsNotNull(yyjson_mut_doc *doc, const string &field_name);

// Public API implementation.
yyjson_mut_val *TranslateFilters(yyjson_mut_doc *doc, const TableFilterSet &filters, const vector<string> &column_names,
                                 const unordered_map<string, string> &es_types,
                                 const unordered_set<string> &text_fields) {
	if (filters.filters.empty()) {
		return nullptr;
	}

	// If only one filter, return it directly.
	if (filters.filters.size() == 1) {
		auto &entry = *filters.filters.begin();
		idx_t col_idx = entry.first;
		auto &filter = entry.second;

		if (col_idx >= column_names.size()) {
			return nullptr;
		}

		const string &column_name = column_names[col_idx];

		return TranslateFilter(doc, *filter, column_name, es_types, text_fields);
	}

	// Multiple filters, combine with bool.must (AND).
	yyjson_mut_val *bool_obj = yyjson_mut_obj(doc);
	yyjson_mut_val *must_arr = yyjson_mut_arr(doc);

	for (auto &entry : filters.filters) {
		idx_t col_idx = entry.first;
		auto &filter = entry.second;

		if (col_idx >= column_names.size()) {
			continue;
		}

		const string &column_name = column_names[col_idx];

		yyjson_mut_val *translated = TranslateFilter(doc, *filter, column_name, es_types, text_fields);
		if (translated) {
			yyjson_mut_arr_append(must_arr, translated);
		}
	}

	if (yyjson_mut_arr_size(must_arr) == 0) {
		return nullptr;
	}

	if (yyjson_mut_arr_size(must_arr) == 1) {
		// Single filter, return it directly.
		return yyjson_mut_arr_get_first(must_arr);
	}

	yyjson_mut_obj_add_val(doc, bool_obj, "must", must_arr);

	yyjson_mut_val *result = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, result, "bool", bool_obj);
	return result;
}

// Translate a single filter for a specific column.
static yyjson_mut_val *TranslateFilter(yyjson_mut_doc *doc, const TableFilter &filter, const string &column_name,
                                       const unordered_map<string, string> &es_types,
                                       const unordered_set<string> &text_fields) {
	// Look up the text field status for this column.
	bool is_text_field = text_fields.count(column_name) > 0;

	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &const_filter = filter.Cast<ConstantFilter>();
		return TranslateConstantComparison(doc, const_filter, column_name, is_text_field);
	}

	case TableFilterType::IS_NULL:
		return TranslateIsNull(doc, column_name);

	case TableFilterType::IS_NOT_NULL:
		return TranslateIsNotNull(doc, column_name);

	case TableFilterType::CONJUNCTION_AND: {
		auto &conj_filter = filter.Cast<ConjunctionAndFilter>();
		return TranslateConjunctionAnd(doc, conj_filter, column_name, es_types, text_fields);
	}

	case TableFilterType::CONJUNCTION_OR: {
		auto &conj_filter = filter.Cast<ConjunctionOrFilter>();
		return TranslateConjunctionOr(doc, conj_filter, column_name, es_types, text_fields);
	}

	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		return TranslateInFilter(doc, in_filter, column_name, is_text_field);
	}

	case TableFilterType::EXPRESSION_FILTER: {
		auto &expr_filter = filter.Cast<ExpressionFilter>();
		return TranslateExpressionFilter(doc, expr_filter, column_name, is_text_field);
	}

	case TableFilterType::STRUCT_EXTRACT: {
		// Handle filters on nested struct fields.
		// The StructFilter wraps the child filter with the nested field name.
		auto &struct_filter = filter.Cast<StructFilter>();

		// Build the nested field path.
		string nested_field = column_name + "." + struct_filter.child_name;

		// Recursively translate the child filter with the nested field path.
		// The es_types and text_fields maps may contain entries for nested paths.
		return TranslateFilter(doc, *struct_filter.child_filter, nested_field, es_types, text_fields);
	}

	default:
		// Unsupported filter type, return nullptr (filter will be applied by DuckDB).
		return nullptr;
	}
}

static yyjson_mut_val *TranslateConstantComparison(yyjson_mut_doc *doc, const ConstantFilter &filter,
                                                   const string &field_name, bool is_text_field) {
	string es_field = GetElasticsearchFieldName(field_name, is_text_field);
	yyjson_mut_val *value = DuckDBValueToJson(doc, filter.constant);

	switch (filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL: {
		// {"term": {"field": value}}
		yyjson_mut_val *term_inner = yyjson_mut_obj(doc);
		yyjson_mut_val *key = yyjson_mut_strcpy(doc, es_field.c_str());
		yyjson_mut_obj_add(term_inner, key, value);

		yyjson_mut_val *result = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, result, "term", term_inner);
		return result;
	}

	case ExpressionType::COMPARE_NOTEQUAL: {
		// {"bool": {"must_not": {"term": {"field": value}}}}
		yyjson_mut_val *term_inner = yyjson_mut_obj(doc);
		yyjson_mut_val *key_ne = yyjson_mut_strcpy(doc, es_field.c_str());
		yyjson_mut_obj_add(term_inner, key_ne, value);

		yyjson_mut_val *term = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, term, "term", term_inner);

		yyjson_mut_val *bool_obj = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, bool_obj, "must_not", term);

		yyjson_mut_val *result = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, result, "bool", bool_obj);
		return result;
	}

	// For text fields, use .keyword subfield for range queries.
	// Text fields are analyzed so range queries operate on tokens, not original values.
	// The .keyword subfield stores the original value and supports proper range queries.
	case ExpressionType::COMPARE_GREATERTHAN: {
		// {"range": {"field": {"gt": value}}}
		yyjson_mut_val *range_cond = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, range_cond, "gt", value);

		yyjson_mut_val *range_inner = yyjson_mut_obj(doc);
		yyjson_mut_val *key_gt = yyjson_mut_strcpy(doc, es_field.c_str());
		yyjson_mut_obj_add(range_inner, key_gt, range_cond);

		yyjson_mut_val *result = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, result, "range", range_inner);
		return result;
	}

	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		// {"range": {"field": {"gte": value}}}
		yyjson_mut_val *range_cond = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, range_cond, "gte", value);

		yyjson_mut_val *range_inner = yyjson_mut_obj(doc);
		yyjson_mut_val *key_gte = yyjson_mut_strcpy(doc, es_field.c_str());
		yyjson_mut_obj_add(range_inner, key_gte, range_cond);

		yyjson_mut_val *result = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, result, "range", range_inner);
		return result;
	}

	case ExpressionType::COMPARE_LESSTHAN: {
		// {"range": {"field": {"lt": value}}}
		yyjson_mut_val *range_cond = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, range_cond, "lt", value);

		yyjson_mut_val *range_inner = yyjson_mut_obj(doc);
		yyjson_mut_val *key_lt = yyjson_mut_strcpy(doc, es_field.c_str());
		yyjson_mut_obj_add(range_inner, key_lt, range_cond);

		yyjson_mut_val *result = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, result, "range", range_inner);
		return result;
	}

	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		// {"range": {"field": {"lte": value}}}
		yyjson_mut_val *range_cond = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, range_cond, "lte", value);

		yyjson_mut_val *range_inner = yyjson_mut_obj(doc);
		yyjson_mut_val *key_lte = yyjson_mut_strcpy(doc, es_field.c_str());
		yyjson_mut_obj_add(range_inner, key_lte, range_cond);

		yyjson_mut_val *result = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, result, "range", range_inner);
		return result;
	}

	default:
		// Unsupported comparison type.
		return nullptr;
	}
}

static yyjson_mut_val *TranslateIsNull(yyjson_mut_doc *doc, const string &field_name) {
	// {"bool": {"must_not": {"exists": {"field": "name"}}}}
	yyjson_mut_val *exists_inner = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_strcpy(doc, exists_inner, "field", field_name.c_str());

	yyjson_mut_val *exists = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, exists, "exists", exists_inner);

	yyjson_mut_val *bool_obj = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, bool_obj, "must_not", exists);

	yyjson_mut_val *result = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, result, "bool", bool_obj);
	return result;
}

static yyjson_mut_val *TranslateIsNotNull(yyjson_mut_doc *doc, const string &field_name) {
	// {"exists": {"field": "name"}}
	yyjson_mut_val *exists_inner = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_strcpy(doc, exists_inner, "field", field_name.c_str());

	yyjson_mut_val *result = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, result, "exists", exists_inner);
	return result;
}

static yyjson_mut_val *TranslateConjunctionAnd(yyjson_mut_doc *doc, const ConjunctionAndFilter &filter,
                                               const string &column_name, const unordered_map<string, string> &es_types,
                                               const unordered_set<string> &text_fields) {
	// {"bool": {"must": [filter1, filter2, ...]}}
	yyjson_mut_val *must_arr = yyjson_mut_arr(doc);

	for (auto &child_filter : filter.child_filters) {
		yyjson_mut_val *translated = TranslateFilter(doc, *child_filter, column_name, es_types, text_fields);
		if (translated) {
			yyjson_mut_arr_append(must_arr, translated);
		}
	}

	if (yyjson_mut_arr_size(must_arr) == 0) {
		return nullptr;
	}

	if (yyjson_mut_arr_size(must_arr) == 1) {
		return yyjson_mut_arr_get_first(must_arr);
	}

	yyjson_mut_val *bool_obj = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, bool_obj, "must", must_arr);

	yyjson_mut_val *result = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, result, "bool", bool_obj);
	return result;
}

static yyjson_mut_val *TranslateConjunctionOr(yyjson_mut_doc *doc, const ConjunctionOrFilter &filter,
                                              const string &column_name, const unordered_map<string, string> &es_types,
                                              const unordered_set<string> &text_fields) {
	// {"bool": {"should": [filter1, filter2, ...], "minimum_should_match": 1}}
	yyjson_mut_val *should_arr = yyjson_mut_arr(doc);

	for (auto &child_filter : filter.child_filters) {
		yyjson_mut_val *translated = TranslateFilter(doc, *child_filter, column_name, es_types, text_fields);
		if (translated) {
			yyjson_mut_arr_append(should_arr, translated);
		}
	}

	if (yyjson_mut_arr_size(should_arr) == 0) {
		return nullptr;
	}

	if (yyjson_mut_arr_size(should_arr) == 1) {
		return yyjson_mut_arr_get_first(should_arr);
	}

	yyjson_mut_val *bool_obj = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, bool_obj, "should", should_arr);
	yyjson_mut_obj_add_int(doc, bool_obj, "minimum_should_match", 1);

	yyjson_mut_val *result = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, result, "bool", bool_obj);
	return result;
}

static yyjson_mut_val *TranslateInFilter(yyjson_mut_doc *doc, const InFilter &filter, const string &field_name,
                                         bool is_text_field) {
	// {"terms": {"field": [value1, value2, ...]}}
	// or for text fields: {"terms": {"field.keyword": [value1, value2, ...]}}
	string es_field = GetElasticsearchFieldName(field_name, is_text_field);

	yyjson_mut_val *values_arr = yyjson_mut_arr(doc);
	for (auto &value : filter.values) {
		yyjson_mut_val *json_val = DuckDBValueToJson(doc, value);
		yyjson_mut_arr_append(values_arr, json_val);
	}

	yyjson_mut_val *terms_inner = yyjson_mut_obj(doc);
	yyjson_mut_val *key_in = yyjson_mut_strcpy(doc, es_field.c_str());
	yyjson_mut_obj_add(terms_inner, key_in, values_arr);

	yyjson_mut_val *result = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, result, "terms", terms_inner);
	return result;
}

static yyjson_mut_val *TranslateExpressionFilter(yyjson_mut_doc *doc, const ExpressionFilter &filter,
                                                 const string &column_name, bool is_text_field) {
	// ExpressionFilter contains arbitrary expressions. We handle:
	// - LIKE/ILIKE patterns (~~, ~~~, like_escape, ilike_escape)
	// - Optimized string functions from LikeOptimizationRule (prefix, suffix, contains)
	//
	// Note: For text fields, only ILIKE patterns are pushed down (by ElasticsearchPushdownComplexFilter).
	// LIKE patterns and optimized functions (prefix, suffix, contains) are case-sensitive and
	// cannot be correctly implemented on analyzed text fields, so they are not pushed down.
	auto &expr = *filter.expr;

	// Check if this is a function expression.
	if (expr.type == ExpressionType::BOUND_FUNCTION) {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		auto func_name = func_expr.function.name;

		// Handle LIKE and ILIKE.
		// For text fields, only ILIKE (~~*, ilike_escape) will reach here.
		// For non-text fields, both LIKE and ILIKE will reach here.
		if (func_name == "~~" || func_name == "like_escape" || func_name == "~~*" || func_name == "ilike_escape") {
			// LIKE pattern is typically the second argument (index 1).
			// First argument (index 0) is the column reference.
			if (func_expr.children.size() >= 2) {
				auto &pattern_expr = func_expr.children[1];
				if (pattern_expr->type == ExpressionType::VALUE_CONSTANT) {
					auto &const_expr = pattern_expr->Cast<BoundConstantExpression>();
					if (const_expr.value.type().id() == LogicalTypeId::VARCHAR) {
						string pattern = StringValue::Get(const_expr.value);
						// ~~* and ilike_escape are case-insensitive (ILIKE)
						// ~~ and like_escape are case-sensitive (LIKE)
						bool case_insensitive = (func_name == "~~*" || func_name == "ilike_escape");
						return TranslateLikePattern(doc, column_name, pattern, is_text_field, case_insensitive);
					}
				}
			}
		}

		// Handle optimized string functions from DuckDB's LikeOptimizationRule:
		// - prefix(col, 'str') from LIKE 'str%'
		// - suffix(col, 'str') from LIKE '%str'
		// - contains(col, 'str') from LIKE '%str%'
		// These always come from case-sensitive LIKE (not ILIKE), so case_insensitive = false.
		// For text fields, these are NOT pushed down by ElasticsearchPushdownComplexFilter,
		// so we only receive them for non-text fields (keyword, etc.)
		if (func_name == "prefix" || func_name == "suffix" || func_name == "contains") {
			if (func_expr.children.size() >= 2) {
				auto &value_expr = func_expr.children[1];
				if (value_expr->type == ExpressionType::VALUE_CONSTANT) {
					auto &const_expr = value_expr->Cast<BoundConstantExpression>();
					if (const_expr.value.type().id() == LogicalTypeId::VARCHAR) {
						string value = StringValue::Get(const_expr.value);

						// Convert to equivalent LIKE pattern and use existing translation.
						// prefix(col, 'str') -> 'str%'
						// suffix(col, 'str') -> '%str'
						// contains(col, 'str') -> '%str%'
						string pattern;
						if (func_name == "prefix") {
							pattern = value + "%";
						} else if (func_name == "suffix") {
							pattern = "%" + value;
						} else { // contains
							pattern = "%" + value + "%";
						}
						// These come from LIKE optimization, so they are case-sensitive.
						return TranslateLikePattern(doc, column_name, pattern, is_text_field, false);
					}
				}
			}
		}
	}

	// Unsupported expression, return nullptr (DuckDB will evaluate it).
	return nullptr;
}

static yyjson_mut_val *TranslateLikePattern(yyjson_mut_doc *doc, const string &field_name, const string &pattern,
                                            bool is_text_field, bool case_insensitive) {
	// Convert SQL LIKE pattern to Elasticsearch wildcard query:
	// SQL LIKE: % = any chars, _ = single char
	// Elasticsearch wildcard: * = any chars, ? = single char
	//
	// Special optimizations:
	// - "prefix%" -> {"prefix": {"field": "prefix"}} (faster than wildcard)
	// - patterns without wildcards -> {"term": {"field": "value"}}

	string es_field = GetElasticsearchFieldName(field_name, is_text_field);

	// Check if pattern has any wildcards.
	bool has_percent = pattern.find('%') != string::npos;
	bool has_underscore = pattern.find('_') != string::npos;

	if (!has_percent && !has_underscore) {
		// No wildcards, treat as exact match.
		yyjson_mut_val *term_inner = yyjson_mut_obj(doc);
		// Use yyjson_mut_strcpy for dynamic key to ensure string is copied.
		yyjson_mut_val *field_key = yyjson_mut_strcpy(doc, es_field.c_str());
		yyjson_mut_val *field_val = yyjson_mut_strcpy(doc, pattern.c_str());
		yyjson_mut_obj_add(term_inner, field_key, field_val);

		yyjson_mut_val *result = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, result, "term", term_inner);
		return result;
	}

	// Check for simple prefix pattern ("prefix%").
	if (has_percent && !has_underscore) {
		size_t percent_pos = pattern.find('%');
		// Check if there is only one % and it's at the end.
		if (percent_pos == pattern.length() - 1 && pattern.find('%') == pattern.rfind('%')) {
			// Simple prefix query.
			string prefix = pattern.substr(0, percent_pos);

			if (is_text_field) {
				// For text fields, use match_phrase_prefix which works with analyzed text
				// and is case-insensitive by default.
				// {"match_phrase_prefix": {"field": "prefix"}}
				yyjson_mut_val *match_inner = yyjson_mut_obj(doc);
				yyjson_mut_val *field_key = yyjson_mut_strcpy(doc, field_name.c_str());
				yyjson_mut_val *field_val = yyjson_mut_strcpy(doc, prefix.c_str());
				yyjson_mut_obj_add(match_inner, field_key, field_val);

				yyjson_mut_val *result = yyjson_mut_obj(doc);
				yyjson_mut_obj_add_val(doc, result, "match_phrase_prefix", match_inner);
				return result;
			} else {
				// For keyword fields, use prefix query on the base field.
				// {"prefix": {"field": "prefix"}}
				yyjson_mut_val *prefix_inner = yyjson_mut_obj(doc);
				yyjson_mut_val *field_key = yyjson_mut_strcpy(doc, field_name.c_str());
				yyjson_mut_val *field_val = yyjson_mut_strcpy(doc, prefix.c_str());
				yyjson_mut_obj_add(prefix_inner, field_key, field_val);

				yyjson_mut_val *result = yyjson_mut_obj(doc);
				yyjson_mut_obj_add_val(doc, result, "prefix", prefix_inner);
				return result;
			}
		}
	}

	// Convert LIKE pattern to Elasticsearch wildcard pattern.
	string es_pattern;
	es_pattern.reserve(pattern.length());

	bool escape_next = false;
	for (size_t i = 0; i < pattern.length(); i++) {
		char c = pattern[i];

		if (escape_next) {
			// Previous char was escape, add this char literally.
			// Escape special Elasticsearch wildcard chars if needed.
			if (c == '*' || c == '?') {
				es_pattern += '\\';
			}
			es_pattern += c;
			escape_next = false;
			continue;
		}

		switch (c) {
		case '%':
			es_pattern += '*';
			break;
		case '_':
			es_pattern += '?';
			break;
		case '\\':
			// SQL escape character.
			escape_next = true;
			break;
		case '*':
		case '?':
			// Escape Elasticsearch wildcard characters that appear literally in the pattern.
			es_pattern += '\\';
			es_pattern += c;
			break;
		default:
			es_pattern += c;
			break;
		}
	}

	// For keyword fields, use the field directly.
	// {"wildcard": {"field": {"value": "pattern"}}}
	// For text fields with case_insensitive=true (ILIKE), use case_insensitive option.
	// {"wildcard": {"field": {"value": "pattern", "case_insensitive": true}}}
	yyjson_mut_val *wildcard_value = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_strcpy(doc, wildcard_value, "value", es_pattern.c_str());
	if (case_insensitive) {
		yyjson_mut_obj_add_bool(doc, wildcard_value, "case_insensitive", true);
	}

	yyjson_mut_val *wildcard_inner = yyjson_mut_obj(doc);
	// For text fields use base field name, for keyword fields use as-is.
	yyjson_mut_val *field_key = yyjson_mut_strcpy(doc, field_name.c_str());
	yyjson_mut_obj_add(wildcard_inner, field_key, wildcard_value);

	yyjson_mut_val *result = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, result, "wildcard", wildcard_inner);
	return result;
}

} // namespace duckdb
