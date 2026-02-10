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
                                       const unordered_set<string> &text_fields,
                                       const unordered_set<string> &text_fields_with_keyword);

static yyjson_mut_val *TranslateConstantComparison(yyjson_mut_doc *doc, const ConstantFilter &filter,
                                                   const string &field_name, bool is_text_field,
                                                   bool has_keyword_subfield);

static yyjson_mut_val *TranslateConjunctionAnd(yyjson_mut_doc *doc, const ConjunctionAndFilter &filter,
                                               const string &column_name, const unordered_map<string, string> &es_types,
                                               const unordered_set<string> &text_fields,
                                               const unordered_set<string> &text_fields_with_keyword);

static yyjson_mut_val *TranslateConjunctionOr(yyjson_mut_doc *doc, const ConjunctionOrFilter &filter,
                                              const string &column_name, const unordered_map<string, string> &es_types,
                                              const unordered_set<string> &text_fields,
                                              const unordered_set<string> &text_fields_with_keyword);

static yyjson_mut_val *TranslateInFilter(yyjson_mut_doc *doc, const InFilter &filter, const string &field_name,
                                         bool is_text_field, bool has_keyword_subfield);

static yyjson_mut_val *TranslateExpressionFilter(yyjson_mut_doc *doc, const ExpressionFilter &filter,
                                                 const string &column_name, bool is_text_field,
                                                 bool has_keyword_subfield);

static yyjson_mut_val *TranslateLikePattern(yyjson_mut_doc *doc, const string &field_name, const string &pattern,
                                            bool is_text_field, bool has_keyword_subfield, bool case_insensitive);

static yyjson_mut_val *TranslateIsNull(yyjson_mut_doc *doc, const string &field_name);

static yyjson_mut_val *TranslateIsNotNull(yyjson_mut_doc *doc, const string &field_name);

// Public API implementation.
FilterTranslationResult TranslateFilters(yyjson_mut_doc *doc, const TableFilterSet &filters,
                                         const vector<string> &column_names,
                                         const unordered_map<string, string> &es_types,
                                         const unordered_set<string> &text_fields,
                                         const unordered_set<string> &text_fields_with_keyword) {
	FilterTranslationResult result;
	result.es_query = nullptr;

	if (filters.filters.empty()) {
		return result;
	}

	// Collect translated filters.
	yyjson_mut_val *bool_obj = yyjson_mut_obj(doc);
	yyjson_mut_val *must_arr = yyjson_mut_arr(doc);

	for (auto &entry : filters.filters) {
		idx_t col_idx = entry.first;
		auto &filter = entry.second;

		if (col_idx >= column_names.size()) {
			continue;
		}

		const string &column_name = column_names[col_idx];

		yyjson_mut_val *translated =
		    TranslateFilter(doc, *filter, column_name, es_types, text_fields, text_fields_with_keyword);
		if (translated) {
			yyjson_mut_arr_append(must_arr, translated);
		}
		// Note: TranslateFilter now throws an error for unsupported filters on text fields
		// without .keyword, so we don't need to track non-translated filters anymore.
	}

	if (yyjson_mut_arr_size(must_arr) == 0) {
		return result;
	}

	if (yyjson_mut_arr_size(must_arr) == 1) {
		// Single filter, return it directly.
		result.es_query = yyjson_mut_arr_get_first(must_arr);
		return result;
	}

	yyjson_mut_obj_add_val(doc, bool_obj, "must", must_arr);

	yyjson_mut_val *query = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, query, "bool", bool_obj);
	result.es_query = query;
	return result;
}

// Translate a single filter for a specific column.
static yyjson_mut_val *TranslateFilter(yyjson_mut_doc *doc, const TableFilter &filter, const string &column_name,
                                       const unordered_map<string, string> &es_types,
                                       const unordered_set<string> &text_fields,
                                       const unordered_set<string> &text_fields_with_keyword) {
	// Look up the text field status for this column.
	bool is_text_field = text_fields.count(column_name) > 0;
	bool has_keyword_subfield = text_fields_with_keyword.count(column_name) > 0;

	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &const_filter = filter.Cast<ConstantFilter>();
		return TranslateConstantComparison(doc, const_filter, column_name, is_text_field, has_keyword_subfield);
	}

	case TableFilterType::IS_NULL:
		return TranslateIsNull(doc, column_name);

	case TableFilterType::IS_NOT_NULL:
		return TranslateIsNotNull(doc, column_name);

	case TableFilterType::CONJUNCTION_AND: {
		auto &conj_filter = filter.Cast<ConjunctionAndFilter>();
		return TranslateConjunctionAnd(doc, conj_filter, column_name, es_types, text_fields, text_fields_with_keyword);
	}

	case TableFilterType::CONJUNCTION_OR: {
		auto &conj_filter = filter.Cast<ConjunctionOrFilter>();
		return TranslateConjunctionOr(doc, conj_filter, column_name, es_types, text_fields, text_fields_with_keyword);
	}

	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		return TranslateInFilter(doc, in_filter, column_name, is_text_field, has_keyword_subfield);
	}

	case TableFilterType::EXPRESSION_FILTER: {
		auto &expr_filter = filter.Cast<ExpressionFilter>();
		return TranslateExpressionFilter(doc, expr_filter, column_name, is_text_field, has_keyword_subfield);
	}

	case TableFilterType::STRUCT_EXTRACT: {
		// Handle filters on nested struct fields.
		// The StructFilter wraps the child filter with the nested field name.
		auto &struct_filter = filter.Cast<StructFilter>();

		// Build the nested field path.
		string nested_field = column_name + "." + struct_filter.child_name;

		// Recursively translate the child filter with the nested field path.
		// The es_types and text_fields maps may contain entries for nested paths.
		return TranslateFilter(doc, *struct_filter.child_filter, nested_field, es_types, text_fields,
		                       text_fields_with_keyword);
	}

	default:
		// Unsupported filter type, return nullptr (filter will be applied by DuckDB).
		return nullptr;
	}
}

static yyjson_mut_val *TranslateConstantComparison(yyjson_mut_doc *doc, const ConstantFilter &filter,
                                                   const string &field_name, bool is_text_field,
                                                   bool has_keyword_subfield) {
	// For text fields without .keyword subfield, we cannot push down comparisons.
	// Text fields are analyzed (lowercased, tokenized) and term/range queries on them
	// would not produce correct results. Throw an error with helpful suggestions.
	if (is_text_field && !has_keyword_subfield) {
		throw InvalidInputException("Cannot filter on text field '%s' because it lacks a .keyword subfield. Options:\n"
		                            "  - Add a .keyword subfield to the Elasticsearch mapping\n"
		                            "  - Use the 'query' parameter with native Elasticsearch text queries",
		                            field_name);
	}

	string es_field = GetElasticsearchFieldName(field_name, is_text_field, has_keyword_subfield);
	yyjson_mut_val *value = DuckDBValueToJSON(doc, filter.constant);

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

	// For text fields with .keyword subfield range queries work correctly.
	// The .keyword subfield stores the raw value and supports proper range queries.
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
                                               const unordered_set<string> &text_fields,
                                               const unordered_set<string> &text_fields_with_keyword) {
	// {"bool": {"must": [filter1, filter2, ...]}}
	yyjson_mut_val *must_arr = yyjson_mut_arr(doc);

	for (auto &child_filter : filter.child_filters) {
		yyjson_mut_val *translated =
		    TranslateFilter(doc, *child_filter, column_name, es_types, text_fields, text_fields_with_keyword);
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
                                              const unordered_set<string> &text_fields,
                                              const unordered_set<string> &text_fields_with_keyword) {
	// {"bool": {"should": [filter1, filter2, ...], "minimum_should_match": 1}}
	yyjson_mut_val *should_arr = yyjson_mut_arr(doc);

	for (auto &child_filter : filter.child_filters) {
		yyjson_mut_val *translated =
		    TranslateFilter(doc, *child_filter, column_name, es_types, text_fields, text_fields_with_keyword);
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
                                         bool is_text_field, bool has_keyword_subfield) {
	// For text fields without .keyword subfield, we cannot push down IN filters.
	// Text fields are analyzed (lowercased, tokenized) and terms queries on them
	// would not produce correct results. Throw an error with helpful suggestions.
	if (is_text_field && !has_keyword_subfield) {
		throw InvalidInputException("Cannot filter on text field '%s' because it lacks a .keyword subfield. Options:\n"
		                            "  - Add a .keyword subfield to the Elasticsearch mapping\n"
		                            "  - Use the 'query' parameter with native Elasticsearch text queries",
		                            field_name);
	}

	// {"terms": {"field": [value1, value2, ...]}}
	// or for text fields with .keyword: {"terms": {"field.keyword": [value1, value2, ...]}}
	string es_field = GetElasticsearchFieldName(field_name, is_text_field, has_keyword_subfield);

	yyjson_mut_val *values_arr = yyjson_mut_arr(doc);
	for (auto &value : filter.values) {
		yyjson_mut_val *json_val = DuckDBValueToJSON(doc, value);
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
                                                 const string &column_name, bool is_text_field,
                                                 bool has_keyword_subfield) {
	// ExpressionFilter contains arbitrary expressions. We handle:
	// - LIKE/ILIKE patterns (~~, ~~~, like_escape, ilike_escape)
	// - Optimized string functions from LikeOptimizationRule (prefix, suffix, contains)
	//
	// Note: For text fields without .keyword subfield an error is thrown early in TranslateLikePattern.
	// Only text fields with .keyword or non-text fields (keyword, etc.) reach the pattern translation.
	auto &expr = *filter.expr;

	// Check if this is a function expression.
	if (expr.type == ExpressionType::BOUND_FUNCTION) {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		auto func_name = func_expr.function.name;

		// Handle LIKE (~~, like_escape) and ILIKE (~~*, ilike_escape).
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
						return TranslateLikePattern(doc, column_name, pattern, is_text_field, has_keyword_subfield,
						                            case_insensitive);
					}
				}
			}
		}

		// Handle optimized string functions from DuckDB's LikeOptimizationRule:
		// - prefix(col, 'str') from LIKE 'str%'
		// - suffix(col, 'str') from LIKE '%str'
		// - contains(col, 'str') from LIKE '%str%'
		// These always come from case-sensitive LIKE (not ILIKE), so case_insensitive = false.
		// For text fields without .keyword these are not pushed down by ElasticsearchPushdownComplexFilter.
		// For text fields with .keyword these are pushed down and will use the .keyword subfield.
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
						return TranslateLikePattern(doc, column_name, pattern, is_text_field, has_keyword_subfield,
						                            false);
					}
				}
			}
		}
	}

	// Unsupported expression, return nullptr (DuckDB will evaluate it).
	return nullptr;
}

static yyjson_mut_val *TranslateLikePattern(yyjson_mut_doc *doc, const string &field_name, const string &pattern,
                                            bool is_text_field, bool has_keyword_subfield, bool case_insensitive) {
	// Convert SQL LIKE pattern to Elasticsearch wildcard query:
	// SQL LIKE: % = any chars, _ = single char
	// Elasticsearch wildcard: * = any chars, ? = single char
	//
	// Special optimizations:
	// - "prefix%" -> {"prefix": {"field": "prefix"}} (faster than wildcard)
	// - patterns without wildcards -> {"term": {"field": "value"}}
	//
	// For text fields with .keyword subfield:
	// - LIKE (case-sensitive): use field.keyword for exact matching
	// - ILIKE (case-insensitive): use field.keyword with case_insensitive option
	//
	// Throw an error for text fields without .keyword subfield.

	// For text fields without .keyword, LIKE/ILIKE patterns are not supported.
	// Text fields are analyzed (tokenized, lowercased) so pattern matching doesn't work correctly.
	if (is_text_field && !has_keyword_subfield) {
		throw InvalidInputException("Cannot filter on text field '%s' because it lacks a .keyword subfield. Options:\n"
		                            "  - Add a .keyword subfield to the Elasticsearch mapping\n"
		                            "  - Use the 'query' parameter with native Elasticsearch text queries",
		                            field_name);
	}

	// Determine the field to use for the query.
	string es_field;
	if (is_text_field && has_keyword_subfield) {
		// Text field with .keyword subfield, always use .keyword for both LIKE and ILIKE.
		es_field = field_name + ".keyword";
	} else {
		// Non-text field (keyword, etc.), use base field.
		es_field = field_name;
	}

	// Check if pattern has any wildcards.
	bool has_percent = pattern.find('%') != string::npos;
	bool has_underscore = pattern.find('_') != string::npos;

	if (!has_percent && !has_underscore) {
		// No wildcards, treat as exact match.
		yyjson_mut_val *term_inner = yyjson_mut_obj(doc);
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
			// For keyword fields and text fields with .keyword, use prefix query.
			// {"prefix": {"field": {"value": "prefix"}}} or with case_insensitive option
			string prefix = pattern.substr(0, percent_pos);

			yyjson_mut_val *prefix_opts = yyjson_mut_obj(doc);
			yyjson_mut_obj_add_strcpy(doc, prefix_opts, "value", prefix.c_str());
			if (case_insensitive) {
				yyjson_mut_obj_add_bool(doc, prefix_opts, "case_insensitive", true);
			}

			yyjson_mut_val *prefix_inner = yyjson_mut_obj(doc);
			yyjson_mut_val *field_key = yyjson_mut_strcpy(doc, es_field.c_str());
			yyjson_mut_obj_add(prefix_inner, field_key, prefix_opts);

			yyjson_mut_val *result = yyjson_mut_obj(doc);
			yyjson_mut_obj_add_val(doc, result, "prefix", prefix_inner);
			return result;
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

	// Use wildcard query.
	// {"wildcard": {"field": {"value": "pattern"}}} or with case_insensitive option
	yyjson_mut_val *wildcard_value = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_strcpy(doc, wildcard_value, "value", es_pattern.c_str());
	if (case_insensitive) {
		yyjson_mut_obj_add_bool(doc, wildcard_value, "case_insensitive", true);
	}

	yyjson_mut_val *wildcard_inner = yyjson_mut_obj(doc);
	yyjson_mut_val *field_key = yyjson_mut_strcpy(doc, es_field.c_str());
	yyjson_mut_obj_add(wildcard_inner, field_key, wildcard_value);

	yyjson_mut_val *result = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, result, "wildcard", wildcard_inner);
	return result;
}

} // namespace duckdb
