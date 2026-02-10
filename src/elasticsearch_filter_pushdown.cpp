#include "elasticsearch_filter_pushdown.hpp"
#include "elasticsearch_common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
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
                                                 bool has_keyword_subfield,
                                                 const unordered_map<string, string> &es_types);

static yyjson_mut_val *TranslateLikePattern(yyjson_mut_doc *doc, const string &field_name, const string &pattern,
                                            bool is_text_field, bool has_keyword_subfield, bool case_insensitive);

static yyjson_mut_val *TranslateIsNull(yyjson_mut_doc *doc, const string &field_name);

static yyjson_mut_val *TranslateIsNotNull(yyjson_mut_doc *doc, const string &field_name);

static yyjson_mut_val *TranslateGeospatialFilter(yyjson_mut_doc *doc, const BoundFunctionExpression &func_expr,
                                                 const string &column_name,
                                                 const unordered_map<string, string> &es_types);

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
		return TranslateExpressionFilter(doc, expr_filter, column_name, is_text_field, has_keyword_subfield, es_types);
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
                                                 bool has_keyword_subfield,
                                                 const unordered_map<string, string> &es_types) {
	// ExpressionFilter contains arbitrary expressions. We handle:
	// - LIKE/ILIKE patterns (~~, ~~~, like_escape, ilike_escape)
	// - Optimized string functions from LikeOptimizationRule (prefix, suffix, contains)
	// - Geospatial functions from spatial extension (ST_Within, ST_Intersects, ST_Contains, ST_Disjoint)
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

		// Handle geospatial functions from the spatial extension.
		// These are pushed down by ElasticsearchPushdownComplexFilter and arrive as ExpressionFilter.
		// Note: Spatial extension registers functions with mixed case (e.g. ST_Within).
		string func_name_lower = StringUtil::Lower(func_name);
		if (func_name_lower == "st_within" || func_name_lower == "st_intersects" || func_name_lower == "st_contains" ||
		    func_name_lower == "st_disjoint") {
			return TranslateGeospatialFilter(doc, func_expr, column_name, es_types);
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

// Try to extract a constant GeoJSON string from a spatial expression.
// Recognizes:
// - BoundConstantExpression with VARCHAR type -> treat as GeoJSON string directly
//   (produced by the pushdown stage which pre-converts GEOMETRY blobs to GeoJSON)
// - ST_Point(lon, lat) -> {"type":"Point","coordinates":[lon,lat]}
// - ST_GeomFromGeoJSON('...') -> pass through the GeoJSON string
// Returns the GeoJSON string on success, empty string on failure.
static string ExtractConstantGeoJSON(const Expression &expr) {
	// Handle pre-converted GeoJSON string constants.
	// The pushdown stage (ElasticsearchPushdownComplexFilter) replaces GEOMETRY blob constants
	// with VARCHAR GeoJSON strings before creating the ExpressionFilter.
	string str_val;
	if (ExtractConstantString(expr, str_val)) {
		return str_val;
	}

	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return "";
	}

	auto &func_expr = expr.Cast<BoundFunctionExpression>();
	string func_name = StringUtil::Lower(func_expr.function.name);

	// ST_Point(x, y) -> GeoJSON Point
	if (func_name == "st_point" && func_expr.children.size() == 2) {
		double x, y;
		if (ExtractConstantDouble(*func_expr.children[0], x) && ExtractConstantDouble(*func_expr.children[1], y)) {
			return "{\"type\":\"Point\",\"coordinates\":[" + to_string(x) + "," + to_string(y) + "]}";
		}
		return "";
	}

	// ST_GeomFromGeoJSON('...') with a constant string argument.
	if (func_name == "st_geomfromgeojson" && func_expr.children.size() >= 1) {
		if (ExtractConstantString(*func_expr.children[0], str_val)) {
			return str_val;
		}
		return "";
	}

	return "";
}

// Build a geo_shape query:
// {"geo_shape": {"field_name": {"shape": {GeoJSON}, "relation": "within"}}}
static yyjson_mut_val *BuildGeoShapeQuery(yyjson_mut_doc *doc, const string &field_name, const string &geojson,
                                          const string &relation) {
	// Parse the GeoJSON string into a yyjson value.
	yyjson_doc *geojson_doc = yyjson_read(geojson.c_str(), geojson.size(), 0);
	if (!geojson_doc) {
		return nullptr;
	}

	yyjson_val *geojson_root = yyjson_doc_get_root(geojson_doc);
	yyjson_mut_val *shape_val = yyjson_val_mut_copy(doc, geojson_root);
	yyjson_doc_free(geojson_doc);

	if (!shape_val) {
		return nullptr;
	}

	// {"shape": <GeoJSON>, "relation": "<relation>"}
	yyjson_mut_val *field_obj = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, field_obj, "shape", shape_val);
	yyjson_mut_obj_add_strcpy(doc, field_obj, "relation", relation.c_str());

	// {"field_name": {"shape": ..., "relation": ...}}
	yyjson_mut_val *geo_shape_inner = yyjson_mut_obj(doc);
	yyjson_mut_val *field_key = yyjson_mut_strcpy(doc, field_name.c_str());
	yyjson_mut_obj_add(geo_shape_inner, field_key, field_obj);

	// {"geo_shape": {"field_name": ...}}
	yyjson_mut_val *result = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, result, "geo_shape", geo_shape_inner);
	return result;
}

// Build a geo_distance query:
// {"geo_distance": {"distance": "50000m", "field_name": {"lat": 40.7, "lon": -74.0}}}
static yyjson_mut_val *BuildGeoDistanceQuery(yyjson_mut_doc *doc, const string &field_name, double lat, double lon,
                                             double distance_meters) {
	// Format distance as string with "m" suffix.
	string distance_str = to_string(distance_meters) + "m";

	// Build the location object.
	yyjson_mut_val *location_obj = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_real(doc, location_obj, "lat", lat);
	yyjson_mut_obj_add_real(doc, location_obj, "lon", lon);

	// Build the geo_distance inner object.
	yyjson_mut_val *geo_dist_inner = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_strcpy(doc, geo_dist_inner, "distance", distance_str.c_str());
	yyjson_mut_val *field_key = yyjson_mut_strcpy(doc, field_name.c_str());
	yyjson_mut_obj_add(geo_dist_inner, field_key, location_obj);

	// {"geo_distance": {...}}
	yyjson_mut_val *result = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, result, "geo_distance", geo_dist_inner);
	return result;
}

// Build a geo_bounding_box query:
// {"geo_bounding_box": {"field_name": {"top_left": {"lat": ymax, "lon": xmin},
//                                      "bottom_right": {"lat": ymin, "lon": xmax}}}}
static yyjson_mut_val *BuildGeoBoundingBoxQuery(yyjson_mut_doc *doc, const string &field_name, double xmin, double ymin,
                                                double xmax, double ymax) {
	// Build top_left (max lat, min lon) and bottom_right (min lat, max lon).
	yyjson_mut_val *top_left = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_real(doc, top_left, "lat", ymax);
	yyjson_mut_obj_add_real(doc, top_left, "lon", xmin);

	yyjson_mut_val *bottom_right = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_real(doc, bottom_right, "lat", ymin);
	yyjson_mut_obj_add_real(doc, bottom_right, "lon", xmax);

	// Build the field object with the two corners.
	yyjson_mut_val *field_obj = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, field_obj, "top_left", top_left);
	yyjson_mut_obj_add_val(doc, field_obj, "bottom_right", bottom_right);

	// {"field_name": {"top_left": ..., "bottom_right": ...}}
	yyjson_mut_val *bbox_inner = yyjson_mut_obj(doc);
	yyjson_mut_val *field_key = yyjson_mut_strcpy(doc, field_name.c_str());
	yyjson_mut_obj_add(bbox_inner, field_key, field_obj);

	// {"geo_bounding_box": {"field_name": ...}}
	yyjson_mut_val *result = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, result, "geo_bounding_box", bbox_inner);
	return result;
}

// Translate a geospatial function expression to an Elasticsearch geo query.
//
// Handles:
// - ST_Within(A, B) -> geo_shape (relation depends on which arg is the field)
//                      or geo_bounding_box (if B is ST_MakeEnvelope)
// - ST_Intersects(A, B) -> geo_shape with relation=intersects
// - ST_Contains(A, B) -> geo_shape (relation depends on which arg is the field)
// - ST_Disjoint(A, B) -> geo_shape with relation=disjoint
//
// One argument must be ST_GeomFromGeoJSON(column_ref) (the Elasticsearch field), the other
// must be a constant geometry expression. Functions are symmetric in argument position.
static yyjson_mut_val *TranslateGeospatialFilter(yyjson_mut_doc *doc, const BoundFunctionExpression &func_expr,
                                                 const string &column_name,
                                                 const unordered_map<string, string> &es_types) {
	string func_name = StringUtil::Lower(func_expr.function.name);

	// Determine which child is the geo column reference and which is the constant geometry.
	idx_t geo_col_idx = DConstants::INVALID_INDEX;
	idx_t const_geo_idx = DConstants::INVALID_INDEX;

	for (idx_t i = 0; i < 2 && i < func_expr.children.size(); i++) {
		if (IsGeoColumnRef(*func_expr.children[i])) {
			geo_col_idx = i;
		}
	}

	if (geo_col_idx == DConstants::INVALID_INDEX) {
		return nullptr;
	}

	const_geo_idx = (geo_col_idx == 0) ? 1 : 0;

	// Handle ST_Within, ST_Intersects, ST_Contains, ST_Disjoint -> geo_shape or geo_bounding_box.

	// Extract the constant geometry GeoJSON.
	string const_geojson = ExtractConstantGeoJSON(*func_expr.children[const_geo_idx]);
	if (const_geojson.empty()) {
		return nullptr;
	}

	// Check if the constant geometry is an envelope for bounding box optimization.
	// The pushdown stage converts ST_MakeEnvelope to {"type":"envelope","coordinates":[[xmin,ymax],[xmax,ymin]]}.
	// A Polygon produced by WKT conversion of an envelope is also recognized.
	// This applies when:
	// - Function is ST_Within and the field is "within" the envelope
	// - Function is ST_Contains and the envelope "contains" the field
	bool field_is_within_envelope = false;
	if (func_name == "st_within" && geo_col_idx == 0) {
		field_is_within_envelope = true;
	} else if (func_name == "st_contains" && geo_col_idx == 1) {
		field_is_within_envelope = true;
	}

	if (field_is_within_envelope) {
		// Try to parse as envelope GeoJSON: {"type":"envelope","coordinates":[[xmin,ymax],[xmax,ymin]]}
		yyjson_doc *env_doc = yyjson_read(const_geojson.c_str(), const_geojson.size(), 0);
		if (env_doc) {
			yyjson_val *env_root = yyjson_doc_get_root(env_doc);
			yyjson_val *type_val = yyjson_obj_get(env_root, "type");
			if (type_val && string(yyjson_get_str(type_val)) == "envelope") {
				yyjson_val *coords = yyjson_obj_get(env_root, "coordinates");
				if (coords && yyjson_is_arr(coords) && yyjson_arr_size(coords) == 2) {
					yyjson_val *tl = yyjson_arr_get(coords, 0);
					yyjson_val *br = yyjson_arr_get(coords, 1);
					if (tl && br && yyjson_is_arr(tl) && yyjson_is_arr(br) && yyjson_arr_size(tl) >= 2 &&
					    yyjson_arr_size(br) >= 2) {
						double xmin = yyjson_get_num(yyjson_arr_get(tl, 0));
						double ymax = yyjson_get_num(yyjson_arr_get(tl, 1));
						double xmax = yyjson_get_num(yyjson_arr_get(br, 0));
						double ymin = yyjson_get_num(yyjson_arr_get(br, 1));
						yyjson_doc_free(env_doc);
						return BuildGeoBoundingBoxQuery(doc, column_name, xmin, ymin, xmax, ymax);
					}
				}
			}
			yyjson_doc_free(env_doc);
		}
	}

	// Determine the Elasticsearch relation based on the DuckDB function and argument positions.
	// ST_Within(A, B) = "A is within B"
	// ST_Contains(A, B) = "A contains B"
	// ST_Intersects and ST_Disjoint are symmetric.
	string relation;
	if (func_name == "st_within") {
		if (geo_col_idx == 0) {
			// ST_Within(field, shape) -> field is within shape -> relation=within
			relation = "within";
		} else {
			// ST_Within(shape, field) -> shape is within field -> field contains shape -> relation=contains
			relation = "contains";
		}
	} else if (func_name == "st_contains") {
		if (geo_col_idx == 0) {
			// ST_Contains(field, shape) -> field contains shape -> relation=contains
			relation = "contains";
		} else {
			// ST_Contains(shape, field) -> shape contains field -> field is within shape -> relation=within
			relation = "within";
		}
	} else if (func_name == "st_intersects") {
		relation = "intersects";
	} else if (func_name == "st_disjoint") {
		relation = "disjoint";
	} else {
		return nullptr;
	}

	return BuildGeoShapeQuery(doc, column_name, const_geojson, relation);
}

} // namespace duckdb
