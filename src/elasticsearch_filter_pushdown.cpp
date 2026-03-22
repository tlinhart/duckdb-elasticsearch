#include "elasticsearch_filter_pushdown.hpp"
#include "elasticsearch_common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

using namespace duckdb_yyjson;

// ============================================================================
// Filter translation (TableFilter -> Elasticsearch Query DSL)
// ============================================================================

// Get Elasticsearch field name adding .keyword suffix for text fields that have a .keyword subfield.
// For text fields without .keyword subfield returns the base field name (caller should handle appropriately).
static std::string GetElasticsearchFieldName(const std::string &column_name, bool is_text_field,
                                             bool has_keyword_subfield) {
	if (is_text_field && has_keyword_subfield) {
		return column_name + ".keyword";
	}
	return column_name;
}

// Forward declarations of static helper functions.
static yyjson_mut_val *TranslateFilter(yyjson_mut_doc *doc, const TableFilter &filter, const string &column_name,
                                       const ElasticsearchSchema &schema);

static yyjson_mut_val *TranslateConjunctionAnd(yyjson_mut_doc *doc, const ConjunctionAndFilter &filter,
                                               const string &column_name, const ElasticsearchSchema &schema);

static yyjson_mut_val *TranslateConjunctionOr(yyjson_mut_doc *doc, const ConjunctionOrFilter &filter,
                                              const string &column_name, const ElasticsearchSchema &schema);

static yyjson_mut_val *TranslateExpressionFilter(yyjson_mut_doc *doc, const ExpressionFilter &filter,
                                                 const string &column_name, const ElasticsearchSchema &schema);

static yyjson_mut_val *TranslateLikePattern(yyjson_mut_doc *doc, const string &field_name, const string &pattern,
                                            const ElasticsearchSchema &schema, bool case_insensitive);

static yyjson_mut_val *TranslateGeospatialFilter(yyjson_mut_doc *doc, const BoundFunctionExpression &func_expr,
                                                 const string &column_name);

static yyjson_mut_val *TranslateGeoDistanceComparison(yyjson_mut_doc *doc, const BoundComparisonExpression &comp_expr,
                                                      const string &column_name);

static yyjson_mut_val *TranslateGeoDistanceDWithin(yyjson_mut_doc *doc, const BoundFunctionExpression &func_expr,
                                                   const string &column_name);

// Resolve a column expression (BOUND_REF leaf or struct_extract chain over a BOUND_REF leaf) to a
// dotted Elasticsearch field path, anchored at base_name (the column name from TranslateFilter).
// Returns true on success and writes the dotted path into out_path.
//
// Examples (with base_name = "employee"):
// - BOUND_REF(0)                                              -> "employee"
// - struct_extract(BOUND_REF(0), 'name')                      -> "employee.name"
// - struct_extract(struct_extract(BOUND_REF(0), 'a'), 'b')    -> "employee.a.b"
static bool ExtractDottedFieldPath(const Expression &expr, const string &base_name, string &out_path) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		out_path = base_name;
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.function.name != "struct_extract" || func.children.size() != 2) {
			return false;
		}
		if (func.children[1]->type != ExpressionType::VALUE_CONSTANT) {
			return false;
		}
		auto &name_const = func.children[1]->Cast<BoundConstantExpression>();
		if (name_const.value.type().id() != LogicalTypeId::VARCHAR) {
			return false;
		}
		string parent;
		if (!ExtractDottedFieldPath(*func.children[0], base_name, parent)) {
			return false;
		}
		out_path = parent + "." + StringValue::Get(name_const.value);
		return true;
	}
	return false;
}

// Translate a comparison "<column> <op> <constant>" (or its flipped form) on a non-geo, non-text-without-keyword
// field into the appropriate Elasticsearch term/range query.
static yyjson_mut_val *TranslateScalarComparison(yyjson_mut_doc *doc, const string &field_name,
                                                 ExpressionType comparison_type, const Value &constant,
                                                 const ElasticsearchSchema &schema) {
	bool is_text_field = schema.text_fields.count(field_name) > 0;
	bool has_keyword_subfield = schema.text_fields_with_keyword.count(field_name) > 0;

	// Defense-in-depth: text fields without .keyword and geo fields should never reach here
	// (the guard filter in pushdown_complex_filter prevents the FilterCombiner from re-pushing
	// comparisons on them). If they somehow do, return nullptr so DuckDB handles the filter.
	if (is_text_field && !has_keyword_subfield) {
		return nullptr;
	}
	if (schema.geo_fields.count(field_name) > 0) {
		return nullptr;
	}

	string es_field = GetElasticsearchFieldName(field_name, is_text_field, has_keyword_subfield);
	yyjson_mut_val *value = ConvertDuckDBToJSON(doc, constant);

	switch (comparison_type) {
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
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		// {"range": {"field": {"<op>": value}}}
		const char *range_op;
		switch (comparison_type) {
		case ExpressionType::COMPARE_GREATERTHAN:
			range_op = "gt";
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			range_op = "gte";
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			range_op = "lt";
			break;
		default:
			range_op = "lte";
			break;
		}
		yyjson_mut_val *range_cond = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, range_cond, range_op, value);
		yyjson_mut_val *range_inner = yyjson_mut_obj(doc);
		yyjson_mut_val *key = yyjson_mut_strcpy(doc, es_field.c_str());
		yyjson_mut_obj_add(range_inner, key, range_cond);
		yyjson_mut_val *result = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, result, "range", range_inner);
		return result;
	}
	default:
		// Unsupported comparison type.
		return nullptr;
	}
}

// Translate IS NULL: Elasticsearch has no SQL NULL, so this matches documents where the field
// does not exist.
// {"bool": {"must_not": {"exists": {"field": "name"}}}}
static yyjson_mut_val *TranslateIsNull(yyjson_mut_doc *doc, const string &field_name) {
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

// Translate IS NOT NULL: matches documents where the field exists.
// {"exists": {"field": "name"}}
static yyjson_mut_val *TranslateIsNotNull(yyjson_mut_doc *doc, const string &field_name) {
	yyjson_mut_val *exists_inner = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_strcpy(doc, exists_inner, "field", field_name.c_str());
	yyjson_mut_val *result = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, result, "exists", exists_inner);
	return result;
}

// Translate an IN expression "<column> IN (<values...>)" into an Elasticsearch terms query.
static yyjson_mut_val *TranslateInExpression(yyjson_mut_doc *doc, const string &field_name,
                                             const BoundOperatorExpression &op_expr,
                                             const ElasticsearchSchema &schema) {
	bool is_text_field = schema.text_fields.count(field_name) > 0;
	bool has_keyword_subfield = schema.text_fields_with_keyword.count(field_name) > 0;

	// Defense-in-depth: text fields without .keyword and geo fields should never reach here.
	if (is_text_field && !has_keyword_subfield) {
		return nullptr;
	}
	if (schema.geo_fields.count(field_name) > 0) {
		return nullptr;
	}

	// {"terms": {"field": [value1, value2, ...]}} (or "field.keyword" for text fields with .keyword).
	string es_field = GetElasticsearchFieldName(field_name, is_text_field, has_keyword_subfield);

	yyjson_mut_val *values_arr = yyjson_mut_arr(doc);
	for (idx_t i = 1; i < op_expr.children.size(); i++) {
		if (op_expr.children[i]->type != ExpressionType::VALUE_CONSTANT) {
			return nullptr;
		}
		auto &const_expr = op_expr.children[i]->Cast<BoundConstantExpression>();
		yyjson_mut_arr_append(values_arr, ConvertDuckDBToJSON(doc, const_expr.value));
	}

	yyjson_mut_val *terms_inner = yyjson_mut_obj(doc);
	yyjson_mut_val *key_in = yyjson_mut_strcpy(doc, es_field.c_str());
	yyjson_mut_obj_add(terms_inner, key_in, values_arr);

	yyjson_mut_val *result = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, result, "terms", terms_inner);
	return result;
}

FilterTranslationResult TranslateFilters(yyjson_mut_doc *doc, const TableFilterSet &filters,
                                         const vector<string> &column_names, const ElasticsearchSchema &schema) {
	FilterTranslationResult result;
	result.es_query = nullptr;

	if (!filters.HasFilters()) {
		return result;
	}

	// Collect translated filters.
	yyjson_mut_val *bool_obj = yyjson_mut_obj(doc);
	yyjson_mut_val *must_arr = yyjson_mut_arr(doc);

	for (auto &entry : filters) {
		idx_t col_idx = entry.GetIndex();
		auto &filter = entry.Filter();

		if (col_idx >= column_names.size()) {
			continue;
		}

		const string &column_name = column_names[col_idx];

		yyjson_mut_val *translated = TranslateFilter(doc, filter, column_name, schema);
		if (translated) {
			yyjson_mut_arr_append(must_arr, translated);
		}
		// Note: TranslateFilter returns nullptr for unsupported TableFilterTypes or unrecognized
		// expression shapes inside an ExpressionFilter. Such filters are evaluated by DuckDB's
		// FILTER stage above the scan instead.
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
// We translate ExpressionFilter and conjunction filters; any other TableFilterType (e.g.
// ConstantFilter / InFilter / IsNullFilter that DuckDB's FilterCombiner produces when our
// table_filters is empty after the pushdown stage) returns nullptr so DuckDB evaluates them
// in its FILTER stage above the scan.
static yyjson_mut_val *TranslateFilter(yyjson_mut_doc *doc, const TableFilter &filter, const string &column_name,
                                       const ElasticsearchSchema &schema) {
	switch (filter.filter_type) {
	case TableFilterType::EXPRESSION_FILTER:
		return TranslateExpressionFilter(doc, filter.Cast<ExpressionFilter>(), column_name, schema);
	case TableFilterType::CONJUNCTION_AND:
		return TranslateConjunctionAnd(doc, filter.Cast<ConjunctionAndFilter>(), column_name, schema);
	case TableFilterType::CONJUNCTION_OR:
		return TranslateConjunctionOr(doc, filter.Cast<ConjunctionOrFilter>(), column_name, schema);
	default:
		return nullptr;
	}
}

static yyjson_mut_val *TranslateConjunctionAnd(yyjson_mut_doc *doc, const ConjunctionAndFilter &filter,
                                               const string &column_name, const ElasticsearchSchema &schema) {
	// {"bool": {"must": [filter1, filter2, ...]}}
	yyjson_mut_val *must_arr = yyjson_mut_arr(doc);

	for (auto &child_filter : filter.child_filters) {
		yyjson_mut_val *translated = TranslateFilter(doc, *child_filter, column_name, schema);
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
                                              const string &column_name, const ElasticsearchSchema &schema) {
	// {"bool": {"should": [filter1, filter2, ...], "minimum_should_match": 1}}
	yyjson_mut_val *should_arr = yyjson_mut_arr(doc);

	for (auto &child_filter : filter.child_filters) {
		yyjson_mut_val *translated = TranslateFilter(doc, *child_filter, column_name, schema);
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

// Translate the canonical ExpressionFilter shapes produced by the pushdown stage (and by DuckDB's
// FilterCombiner). The expression's column ref leaves are BoundReferenceExpression(0); for nested
// columns the leaf sits at the bottom of a struct_extract chain. We resolve the dotted field path
// from base column_name using ExtractDottedFieldPath.
//
// Recognized shapes:
// - BOUND_COMPARISON: <column> <op> <constant>, including ST_Distance(...) <op> <distance>
// - BOUND_OPERATOR:   IS NULL / IS NOT NULL / COMPARE_IN
// - BOUND_FUNCTION:   LIKE/ILIKE/prefix/suffix/contains and ST_* spatial predicates
//
// The _id always-non-null guard arrives as IS NOT NULL on column_name == "_id"; the optimizer
// extension normally strips it before we get here, but if it survives, it produces a benign
// {"exists":{"field":"_id"}} term.
static yyjson_mut_val *TranslateExpressionFilter(yyjson_mut_doc *doc, const ExpressionFilter &filter,
                                                 const string &column_name, const ElasticsearchSchema &schema) {
	auto &expr = *filter.expr;

	// Comparison: <column> <op> <constant>.
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comp_expr = expr.Cast<BoundComparisonExpression>();

		// ST_Distance comparisons: ST_Distance(geo_col, point) </<=/>/>= distance.
		if (expr.type == ExpressionType::COMPARE_LESSTHAN || expr.type == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
		    expr.type == ExpressionType::COMPARE_GREATERTHAN ||
		    expr.type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
			if ((comp_expr.left->type == ExpressionType::BOUND_FUNCTION &&
			     StringUtil::Lower(comp_expr.left->Cast<BoundFunctionExpression>().function.name) == "st_distance") ||
			    (comp_expr.right->type == ExpressionType::BOUND_FUNCTION &&
			     StringUtil::Lower(comp_expr.right->Cast<BoundFunctionExpression>().function.name) == "st_distance")) {
				return TranslateGeoDistanceComparison(doc, comp_expr, column_name);
			}
		}

		// Standard scalar comparison. One side is a column expression (BOUND_REF or struct_extract chain),
		// the other is a constant.
		const Expression *col_side = nullptr;
		const Expression *const_side = nullptr;
		ExpressionType comparison_type = comp_expr.type;
		if (comp_expr.right->type == ExpressionType::VALUE_CONSTANT) {
			col_side = comp_expr.left.get();
			const_side = comp_expr.right.get();
		} else if (comp_expr.left->type == ExpressionType::VALUE_CONSTANT) {
			col_side = comp_expr.right.get();
			const_side = comp_expr.left.get();
			comparison_type = FlipComparisonExpression(comparison_type);
		} else {
			return nullptr;
		}

		string field_path;
		if (!ExtractDottedFieldPath(*col_side, column_name, field_path)) {
			return nullptr;
		}
		auto &constant = const_side->Cast<BoundConstantExpression>().value;
		if (constant.IsNull()) {
			return nullptr;
		}
		return TranslateScalarComparison(doc, field_path, comparison_type, constant, schema);
	}

	// Operator expressions: IS NULL / IS NOT NULL / COMPARE_IN.
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		auto &op_expr = expr.Cast<BoundOperatorExpression>();
		auto op_type = op_expr.type;

		if ((op_type == ExpressionType::OPERATOR_IS_NULL || op_type == ExpressionType::OPERATOR_IS_NOT_NULL) &&
		    op_expr.children.size() == 1) {
			string field_path;
			if (!ExtractDottedFieldPath(*op_expr.children[0], column_name, field_path)) {
				return nullptr;
			}
			return op_type == ExpressionType::OPERATOR_IS_NULL ? TranslateIsNull(doc, field_path)
			                                                   : TranslateIsNotNull(doc, field_path);
		}

		if (op_type == ExpressionType::COMPARE_IN && op_expr.children.size() >= 2) {
			string field_path;
			if (!ExtractDottedFieldPath(*op_expr.children[0], column_name, field_path)) {
				return nullptr;
			}
			return TranslateInExpression(doc, field_path, op_expr, schema);
		}

		return nullptr;
	}

	// Function expressions: LIKE/ILIKE/prefix/suffix/contains and ST_* spatial predicates.
	if (expr.type == ExpressionType::BOUND_FUNCTION) {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		auto func_name = func_expr.function.name;
		string func_name_lower = StringUtil::Lower(func_name);

		// Spatial predicates and ST_DWithin: dotted path comes from the geo column among the first two args.
		if (func_name_lower == "st_within" || func_name_lower == "st_intersects" || func_name_lower == "st_contains" ||
		    func_name_lower == "st_disjoint") {
			return TranslateGeospatialFilter(doc, func_expr, column_name);
		}
		if (func_name_lower == "st_dwithin") {
			return TranslateGeoDistanceDWithin(doc, func_expr, column_name);
		}

		// LIKE/ILIKE/prefix/suffix/contains. children[0] is the column expression.
		if (func_expr.children.size() < 2) {
			return nullptr;
		}
		auto &pattern_expr = func_expr.children[1];
		if (pattern_expr->type != ExpressionType::VALUE_CONSTANT) {
			return nullptr;
		}
		auto &const_expr = pattern_expr->Cast<BoundConstantExpression>();
		if (const_expr.value.type().id() != LogicalTypeId::VARCHAR) {
			return nullptr;
		}

		string field_path;
		if (!ExtractDottedFieldPath(*func_expr.children[0], column_name, field_path)) {
			return nullptr;
		}

		string value = StringValue::Get(const_expr.value);
		if (func_name == "~~" || func_name == "like_escape" || func_name == "~~*" || func_name == "ilike_escape") {
			// ~~* and ilike_escape are case-insensitive (ILIKE); ~~ and like_escape are case-sensitive (LIKE).
			bool case_insensitive = (func_name == "~~*" || func_name == "ilike_escape");
			return TranslateLikePattern(doc, field_path, value, schema, case_insensitive);
		}
		// prefix/suffix/contains come from DuckDB's LikeOptimizationRule and are always case-sensitive.
		if (func_name == "prefix") {
			return TranslateLikePattern(doc, field_path, value + "%", schema, false);
		}
		if (func_name == "suffix") {
			return TranslateLikePattern(doc, field_path, "%" + value, schema, false);
		}
		if (func_name == "contains") {
			return TranslateLikePattern(doc, field_path, "%" + value + "%", schema, false);
		}
	}

	// Unsupported expression, return nullptr (DuckDB will evaluate it).
	return nullptr;
}

static yyjson_mut_val *TranslateLikePattern(yyjson_mut_doc *doc, const string &field_name, const string &pattern,
                                            const ElasticsearchSchema &schema, bool case_insensitive) {
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

	bool is_text_field = schema.text_fields.count(field_name) > 0;
	bool has_keyword_subfield = schema.text_fields_with_keyword.count(field_name) > 0;

	// Defense-in-depth: text fields without .keyword should never reach here.
	if (is_text_field && !has_keyword_subfield) {
		return nullptr;
	}

	// Determine the field to use for the query.
	string es_field;
	if (is_text_field && has_keyword_subfield) {
		// Text field with .keyword subfield, always use .keyword for both LIKE and ILIKE.
		es_field = field_name + ".keyword";
	} else {
		// Non-text field (keyword etc.), use base field.
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
//   (produced by the pushdown stage which pre-converts GEOMETRY constants to GeoJSON)
// - ST_Point(lon, lat) -> {"type":"Point","coordinates":[lon,lat]}
// - ST_GeomFromGeoJSON('...') -> pass through the GeoJSON string
// Returns the GeoJSON string on success, empty string on failure.
static string ExtractConstantGeoJSON(const Expression &expr) {
	// Handle pre-converted GeoJSON string constants.
	// The pushdown stage (PushdownComplexFilters) replaces GEOMETRY constants
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

// Translate a ST_Distance comparison to an Elasticsearch geo_distance query.
// Handles ST_Distance(geo_col, point) </<=/>/>= distance.
// The pushdown stage has already replaced the GEOMETRY constant with a GeoJSON VARCHAR string.
// For < and <= produce a direct geo_distance query (points within distance).
// For > and >= produce a bool.must_not wrapper around geo_distance (points farther than distance).
static yyjson_mut_val *TranslateGeoDistanceComparison(yyjson_mut_doc *doc, const BoundComparisonExpression &comp_expr,
                                                      const string &column_name) {
	// Identify which side is the ST_Distance function call and which is the distance constant.
	const BoundFunctionExpression *func_expr = nullptr;
	const Expression *dist_expr = nullptr;
	bool func_on_left = false;

	if (comp_expr.left->type == ExpressionType::BOUND_FUNCTION) {
		auto &func = comp_expr.left->Cast<BoundFunctionExpression>();
		if (StringUtil::Lower(func.function.name) == "st_distance" && func.children.size() == 2) {
			func_expr = &func;
			dist_expr = comp_expr.right.get();
			func_on_left = true;
		}
	}
	if (!func_expr && comp_expr.right->type == ExpressionType::BOUND_FUNCTION) {
		auto &func = comp_expr.right->Cast<BoundFunctionExpression>();
		if (StringUtil::Lower(func.function.name) == "st_distance" && func.children.size() == 2) {
			func_expr = &func;
			dist_expr = comp_expr.left.get();
			func_on_left = false;
		}
	}
	if (!func_expr) {
		return nullptr;
	}

	// Extract the distance constant.
	double distance_meters;
	if (!ExtractConstantDouble(*dist_expr, distance_meters)) {
		return nullptr;
	}
	if (distance_meters < 0) {
		return nullptr;
	}

	// From ST_Distance's children, find the constant GeoJSON point and resolve the dotted field path
	// for the geo column.
	idx_t const_child_idx = DConstants::INVALID_INDEX;
	idx_t geo_child_idx = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < 2; i++) {
		if (IsGeoColumnRef(*func_expr->children[i])) {
			geo_child_idx = i;
		} else {
			const_child_idx = i;
		}
	}
	if (const_child_idx == DConstants::INVALID_INDEX || geo_child_idx == DConstants::INVALID_INDEX) {
		return nullptr;
	}

	string field_path;
	if (!ExtractDottedFieldPath(*func_expr->children[geo_child_idx], column_name, field_path)) {
		return nullptr;
	}

	// Extract the GeoJSON string and parse out lat/lon coordinates.
	string point_geojson = ExtractConstantGeoJSON(*func_expr->children[const_child_idx]);
	if (point_geojson.empty()) {
		return nullptr;
	}

	double lon, lat;
	if (!ExtractPointCoordinates(point_geojson, lon, lat)) {
		return nullptr;
	}

	// Normalize operator direction: we want "ST_Distance(...) <operator> distance".
	auto expr_type = comp_expr.GetExpressionType();
	auto comparison_type = func_on_left ? expr_type : FlipComparisonExpression(expr_type);

	// Build the geo_distance query.
	yyjson_mut_val *geo_dist_query = BuildGeoDistanceQuery(doc, field_path, lat, lon, distance_meters);
	if (!geo_dist_query) {
		return nullptr;
	}

	// For < and <= return the geo_distance query directly (points within distance).
	// For > and >= wrap in bool.must_not (points farther than distance).
	if (comparison_type == ExpressionType::COMPARE_LESSTHAN ||
	    comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
		return geo_dist_query;
	} else {
		yyjson_mut_val *bool_obj = yyjson_mut_obj(doc);
		yyjson_mut_val *must_not_arr = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_val(must_not_arr, geo_dist_query);
		yyjson_mut_obj_add_val(doc, bool_obj, "must_not", must_not_arr);
		yyjson_mut_val *result_query = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, result_query, "bool", bool_obj);
		return result_query;
	}
}

// Translate a ST_DWithin function to an Elasticsearch geo_distance query.
// ST_DWithin(geom1, geom2, distance) -> geo_distance query.
// One of geom1/geom2 must be a GEOMETRY column reference, the other a constant point.
// The pushdown stage has already replaced the GEOMETRY constant with a GeoJSON VARCHAR string.
static yyjson_mut_val *TranslateGeoDistanceDWithin(yyjson_mut_doc *doc, const BoundFunctionExpression &func_expr,
                                                   const string &column_name) {
	if (func_expr.children.size() < 3) {
		return nullptr;
	}

	// Find the geo column ref (one of the first two args) and the constant point (the other one).
	idx_t const_child_idx = DConstants::INVALID_INDEX;
	idx_t geo_child_idx = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < 2; i++) {
		if (IsGeoColumnRef(*func_expr.children[i])) {
			geo_child_idx = i;
		} else {
			const_child_idx = i;
		}
	}
	if (const_child_idx == DConstants::INVALID_INDEX || geo_child_idx == DConstants::INVALID_INDEX) {
		return nullptr;
	}

	string field_path;
	if (!ExtractDottedFieldPath(*func_expr.children[geo_child_idx], column_name, field_path)) {
		return nullptr;
	}

	// Extract the GeoJSON string and parse out lat/lon coordinates.
	string point_geojson = ExtractConstantGeoJSON(*func_expr.children[const_child_idx]);
	if (point_geojson.empty()) {
		return nullptr;
	}

	double lon, lat;
	if (!ExtractPointCoordinates(point_geojson, lon, lat)) {
		return nullptr;
	}

	// Extract the distance from the third argument.
	double distance_meters;
	if (!ExtractConstantDouble(*func_expr.children[2], distance_meters)) {
		return nullptr;
	}
	if (distance_meters < 0) {
		return nullptr;
	}

	return BuildGeoDistanceQuery(doc, field_path, lat, lon, distance_meters);
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
// One argument must be a GEOMETRY column reference (the Elasticsearch field), the other
// must be a constant geometry expression. Functions are symmetric in argument position.
static yyjson_mut_val *TranslateGeospatialFilter(yyjson_mut_doc *doc, const BoundFunctionExpression &func_expr,
                                                 const string &column_name) {
	string func_name = StringUtil::Lower(func_expr.function.name);

	if (func_expr.children.size() < 2) {
		return nullptr;
	}

	// Determine which child is the geo column reference and which is the constant geometry.
	idx_t geo_col_idx = DConstants::INVALID_INDEX;
	idx_t const_geo_idx;

	for (idx_t i = 0; i < 2; i++) {
		if (IsGeoColumnRef(*func_expr.children[i])) {
			geo_col_idx = i;
		}
	}

	if (geo_col_idx == DConstants::INVALID_INDEX) {
		return nullptr;
	}

	const_geo_idx = (geo_col_idx == 0) ? 1 : 0;

	// Resolve the dotted field path for the geo column.
	string field_path;
	if (!ExtractDottedFieldPath(*func_expr.children[geo_col_idx], column_name, field_path)) {
		return nullptr;
	}

	// Extract the constant geometry GeoJSON.
	string const_geojson = ExtractConstantGeoJSON(*func_expr.children[const_geo_idx]);
	if (const_geojson.empty()) {
		return nullptr;
	}

	// Check if the constant geometry is an envelope for bounding box optimization.
	// The pushdown stage converts ST_MakeEnvelope and axis-aligned rectangle Polygons to
	// {"type":"envelope","coordinates":[[xmin,ymax],[xmax,ymin]]} before the filter reaches here.
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
						return BuildGeoBoundingBoxQuery(doc, field_path, xmin, ymin, xmax, ymax);
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

	return BuildGeoShapeQuery(doc, field_path, const_geojson, relation);
}

// ============================================================================
// pushdown_complex_filter implementation (expression -> TableFilter conversion)
// ============================================================================

// Result of ExtractColumnPath containing column info needed for filter pushdown.
struct ColumnPathInfo {
	idx_t output_col_idx = DConstants::INVALID_INDEX;
	string full_path;

	bool IsValid() const {
		return output_col_idx != DConstants::INVALID_INDEX;
	}
};

// Extract column path from an expression for filter pushdown.
// Handles direct column references and struct_extract chains for nested object fields.
// Works with both regular column IDs (elasticsearch_query) and virtual column IDs (elasticsearch_aggregate).
//
// Regular column IDs: _id = 0, fields = 1..N, _unmapped_ = N+1
// Virtual column IDs: _id = VIRTUAL_COLUMN_START, fields = VIRTUAL_COLUMN_START+1..+N, _unmapped_ =
// VIRTUAL_COLUMN_START+N+1
//
// Examples:
// - BOUND_COLUMN_REF(col=2) -> {2, "name", []}
// - struct_extract(col, 'name') -> {col_idx, "employee.name", ["name"]}
// - struct_extract(struct_extract(col, 'address'), 'city') -> {col_idx, "employee.address.city", ["address", "city"]}
static ColumnPathInfo ExtractColumnPath(const Expression &expr, const ElasticsearchSchema &schema,
                                        const vector<ColumnIndex> &column_ids) {
	ColumnPathInfo result;

	// Direct column reference.
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		idx_t output_col_idx = col_ref.binding.column_index.GetIndexUnsafe();

		if (output_col_idx >= column_ids.size()) {
			return result;
		}

		const ColumnIndex &col_index = column_ids[output_col_idx];
		idx_t bind_col_id = col_index.GetPrimaryIndex();

		// Handle virtual column IDs (used by elasticsearch_aggregate).
		// Virtual column IDs start at VIRTUAL_COLUMN_START:
		//   VIRTUAL_COLUMN_START + 0 = _id
		//   VIRTUAL_COLUMN_START + 1 + i = schema.column_names[i]
		//   VIRTUAL_COLUMN_START + 1 + schema.column_names.size() = _unmapped_
		if (bind_col_id >= VIRTUAL_COLUMN_START) {
			idx_t virtual_offset = bind_col_id - VIRTUAL_COLUMN_START;
			if (virtual_offset == 0) {
				// _id virtual column
				result.output_col_idx = output_col_idx;
				result.full_path = "_id";
				return result;
			}
			idx_t field_idx = virtual_offset - 1;
			if (field_idx >= schema.column_names.size()) {
				return result; // _unmapped_ or out of range
			}
			result.output_col_idx = output_col_idx;
			result.full_path = schema.column_names[field_idx];
			return result;
		}

		// Regular column IDs (used by elasticsearch_query).
		// _id column has bind_col_id == 0
		if (bind_col_id == 0) {
			result.output_col_idx = output_col_idx;
			result.full_path = "_id";
			return result;
		}

		if (bind_col_id > schema.column_names.size()) {
			return result;
		}

		result.output_col_idx = output_col_idx;
		result.full_path = schema.column_names[bind_col_id - 1];
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
		ColumnPathInfo parent_result = ExtractColumnPath(*func_expr.children[0], schema, column_ids);
		if (!parent_result.IsValid()) {
			return result;
		}

		result.output_col_idx = parent_result.output_col_idx;
		result.full_path = parent_result.full_path + "." + field_name;
		return result;
	}

	return result;
}

// Layout-compatible struct for the spatial extension's ST_DWithin bind data.
// When the distance argument of ST_DWithin is a constant, the spatial extension's Bind function
// erases the third child (distance) from the BoundFunctionExpression and stores the distance value
// in bind_info. This struct mirrors that layout so we can extract the distance.
struct SpatialDWithinBindData : public FunctionData {
	double distance;
	bool is_constant;

	unique_ptr<FunctionData> Copy() const override {
		return nullptr;
	}
	bool Equals(const FunctionData &other) const override {
		return false;
	}
};

// Information about a geospatial column reference in a spatial function argument.
struct GeoColumnInfo {
	ColumnPathInfo col_path;
	// Which child index (0 or 1) of the outer spatial function contains the geo column.
	idx_t arg_index;

	bool IsValid() const {
		return col_path.IsValid();
	}
};

// Extract a geo column reference from a spatial function argument.
// Detects direct BOUND_COLUMN_REF with GEOMETRY type or struct_extract chain
// returning GEOMETRY and verifies the underlying Elasticsearch field is geo_point or geo_shape.
static GeoColumnInfo ExtractGeoColumnFromArg(const Expression &expr, const ElasticsearchSchema &schema,
                                             const vector<ColumnIndex> &column_ids, idx_t arg_index) {
	GeoColumnInfo result;
	result.arg_index = arg_index;

	// The expression must return GEOMETRY type (native geo field).
	if (expr.return_type.id() != LogicalTypeId::GEOMETRY) {
		return result;
	}

	// Extract the column path from the expression (direct column reference or struct_extract chain).
	ColumnPathInfo col_path = ExtractColumnPath(expr, schema, column_ids);
	if (!col_path.IsValid()) {
		return result;
	}

	// Verify the column is a geo_point or geo_shape field.
	const string &col_name = col_path.full_path;
	auto it = schema.es_type_map.find(col_name);
	if (it == schema.es_type_map.end()) {
		return result;
	}
	if (it->second != "geo_point" && it->second != "geo_shape") {
		return result;
	}

	result.col_path = std::move(col_path);
	return result;
}

// Try to extract constant GeoJSON from a spatial expression.
// Recognizes:
// - BoundConstantExpression with GEOMETRY type -> convert WKB to GeoJSON directly
// - ST_Point(lon, lat) -> {"type":"Point","coordinates":[lon,lat]}
// - ST_MakeEnvelope(xmin, ymin, xmax, ymax) -> special "envelope" marker
// - ST_GeomFromGeoJSON('{"type":...}') -> pass through the GeoJSON string
//
// Note: After DuckDB's constant folding, function calls like ST_Point(-74, 40.7)
// are already evaluated to a GEOMETRY constant. The GEOMETRY value is internally WKB
// which we convert directly to GeoJSON via WKBToGeoJSON().
//
// Returns a valid ConstantGeoInfo on success (geojson is set or is_envelope is true and bbox coordinates are set).
struct ConstantGeoInfo {
	string geojson;
	bool is_envelope = false;
	double xmin = 0, ymin = 0, xmax = 0, ymax = 0;

	bool IsValid() const {
		return !geojson.empty() || is_envelope;
	}
};

static ConstantGeoInfo ExtractConstantGeo(const Expression &expr) {
	ConstantGeoInfo result;

	// Handle constant-folded GEOMETRY values.
	// After DuckDB's optimizer constant-folds expressions like ST_Point(-74, 40.7),
	// the result is a BoundConstantExpression with a native GEOMETRY type (WKB binary).
	// We convert the WKB directly to GeoJSON. For axis-aligned rectangles (from
	// ST_MakeEnvelope), we detect the envelope pattern directly from WKB.
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		auto &const_expr = expr.Cast<BoundConstantExpression>();
		if (const_expr.value.IsNull()) {
			return result;
		}
		const auto &val_type = const_expr.value.type();
		if (val_type.id() == LogicalTypeId::GEOMETRY) {
			// Get the WKB binary from the GEOMETRY value.
			const auto &str_val = StringValue::Get(const_expr.value);
			string_t wkb(str_val);

			// Check for axis-aligned rectangle (envelope) directly from WKB.
			double env_xmin, env_ymin, env_xmax, env_ymax;
			if (WKBIsAxisAlignedRectangle(wkb, env_xmin, env_ymin, env_xmax, env_ymax)) {
				result.is_envelope = true;
				result.xmin = env_xmin;
				result.ymin = env_ymin;
				result.xmax = env_xmax;
				result.ymax = env_ymax;
				return result;
			}

			// Convert WKB to GeoJSON for the Elasticsearch query.
			result.geojson = WKBToGeoJSON(wkb);
			return result;
		}
		return result;
	}

	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return result;
	}

	auto &func_expr = expr.Cast<BoundFunctionExpression>();
	string func_name = StringUtil::Lower(func_expr.function.name);

	// ST_Point(x, y) -> GeoJSON Point
	if (func_name == "st_point" && func_expr.children.size() == 2) {
		double x, y;
		if (ExtractConstantDouble(*func_expr.children[0], x) && ExtractConstantDouble(*func_expr.children[1], y)) {
			result.geojson = "{\"type\":\"Point\",\"coordinates\":[" + to_string(x) + "," + to_string(y) + "]}";
			return result;
		}
		return result;
	}

	// ST_MakeEnvelope(xmin, ymin, xmax, ymax) -> bounding box
	if (ExtractEnvelopeCoordinates(expr, result.xmin, result.ymin, result.xmax, result.ymax)) {
		result.is_envelope = true;
		return result;
	}

	// ST_GeomFromGeoJSON('...') with a constant string argument
	if (func_name == "st_geomfromgeojson" && func_expr.children.size() >= 1) {
		string geojson_str;
		if (ExtractConstantString(*func_expr.children[0], geojson_str)) {
			result.geojson = std::move(geojson_str);
			return result;
		}
		return result;
	}

	return result;
}

// Replace any BoundColumnRefExpression nodes inside an expression tree with
// BoundReferenceExpression(index = 0). The resulting expression is suitable as the inner
// expression of an ExpressionFilter, where the only available "column" is the filtered column
// itself (already bound to projection index 0 within the filter).
static void ReplaceColumnRefsWithBoundRefs(unique_ptr<Expression> &root_expr) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    root_expr, [&](BoundColumnRefExpression &col_ref, unique_ptr<Expression> &expr) {
		    expr = make_uniq<BoundReferenceExpression>(col_ref.alias, col_ref.return_type, 0ULL);
	    });
}

// Try to push a simple comparison filter into table_filters.
// This replicates the core logic of FilterCombiner::AddBoundComparisonFilter + TryPushdownConstantFilter.
// We do this in pushdown_complex_filter because pushing any filter to table_filters causes the
// DuckDB optimizer to skip the FilterCombiner path. By also handling comparisons here, all filter types
// are pushed in a single pass.
static bool TryPushComparisonFilter(ClientContext &context, const BoundComparisonExpression &comp_expr,
                                    const ElasticsearchSchema &schema, const vector<ColumnIndex> &column_ids,
                                    LogicalGet &get) {
	auto expr_type = comp_expr.GetExpressionType();
	// Support: =, !=, >, >=, <, <=
	if (expr_type != ExpressionType::COMPARE_EQUAL && expr_type != ExpressionType::COMPARE_NOTEQUAL &&
	    expr_type != ExpressionType::COMPARE_GREATERTHAN && expr_type != ExpressionType::COMPARE_GREATERTHANOREQUALTO &&
	    expr_type != ExpressionType::COMPARE_LESSTHAN && expr_type != ExpressionType::COMPARE_LESSTHANOREQUALTO) {
		return false;
	}

	// One side must be a column reference, the other a foldable scalar.
	bool left_is_scalar = comp_expr.left->IsFoldable();
	bool right_is_scalar = comp_expr.right->IsFoldable();
	if (left_is_scalar == right_is_scalar) {
		// Both scalars (constant expression, not a filter) or both column refs (join condition).
		return false;
	}

	auto &col_expr = left_is_scalar ? *comp_expr.right : *comp_expr.left;
	auto &scalar_expr = left_is_scalar ? *comp_expr.left : *comp_expr.right;

	ColumnPathInfo col_path_info = ExtractColumnPath(col_expr, schema, column_ids);
	if (!col_path_info.IsValid()) {
		return false;
	}

	// Evaluate the scalar side to a constant value.
	Value constant_value;
	if (!ExpressionExecutor::TryEvaluateScalar(context, scalar_expr, constant_value)) {
		return false;
	}
	if (constant_value.IsNull()) {
		return false;
	}

	// If the scalar is on the left side, flip the comparison direction.
	auto comparison_type = left_is_scalar ? FlipComparisonExpression(expr_type) : expr_type;

	// Build "<col_expr_with_bound_ref> <op> <constant>" and wrap in ExpressionFilter.
	auto column_side = col_expr.Copy();
	ReplaceColumnRefsWithBoundRefs(column_side);
	auto constant_side = make_uniq<BoundConstantExpression>(std::move(constant_value));
	auto cmp = make_uniq<BoundComparisonExpression>(comparison_type, std::move(column_side), std::move(constant_side));
	auto filter = make_uniq<ExpressionFilter>(std::move(cmp));

	get.table_filters.PushFilter(ProjectionIndex(col_path_info.output_col_idx), std::move(filter));
	return true;
}

// Try to push a geo_distance filter from a comparison involving ST_Distance.
// Recognizes patterns like ST_Distance(geo_col, ST_Point(...)) < 1000.
// The comparison must have ST_Distance on one side and a numeric constant on the other.
// Only <, <=, >, >= are supported (not = or !=).
static bool TryPushGeoDistanceFilter(ClientContext &context, const BoundComparisonExpression &comp_expr,
                                     const ElasticsearchSchema &schema, const vector<ColumnIndex> &column_ids,
                                     LogicalGet &get) {
	auto expr_type = comp_expr.GetExpressionType();
	if (expr_type != ExpressionType::COMPARE_LESSTHAN && expr_type != ExpressionType::COMPARE_LESSTHANOREQUALTO &&
	    expr_type != ExpressionType::COMPARE_GREATERTHAN && expr_type != ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
		return false;
	}

	// Identify which side is the ST_Distance function and which is the distance constant.
	const Expression *dist_func_expr = nullptr;
	const Expression *dist_value_expr = nullptr;
	bool func_on_left = false;

	if (comp_expr.left->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = comp_expr.left->Cast<BoundFunctionExpression>();
		if (StringUtil::Lower(func.function.name) == "st_distance" && func.children.size() == 2) {
			dist_func_expr = comp_expr.left.get();
			dist_value_expr = comp_expr.right.get();
			func_on_left = true;
		}
	}
	if (!dist_func_expr && comp_expr.right->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = comp_expr.right->Cast<BoundFunctionExpression>();
		if (StringUtil::Lower(func.function.name) == "st_distance" && func.children.size() == 2) {
			dist_func_expr = comp_expr.right.get();
			dist_value_expr = comp_expr.left.get();
			func_on_left = false;
		}
	}
	if (!dist_func_expr) {
		return false;
	}

	// Extract the distance constant.
	if (!dist_value_expr->IsFoldable()) {
		return false;
	}
	Value distance_val;
	if (!ExpressionExecutor::TryEvaluateScalar(context, *dist_value_expr, distance_val)) {
		return false;
	}
	if (distance_val.IsNull()) {
		return false;
	}
	double distance_meters;
	if (!distance_val.DefaultTryCastAs(LogicalType::DOUBLE)) {
		return false;
	}
	distance_meters = DoubleValue::Get(distance_val);
	if (distance_meters < 0) {
		return false;
	}

	// From ST_Distance's children, extract the geo column and the constant point.
	auto &func_expr = dist_func_expr->Cast<BoundFunctionExpression>();

	GeoColumnInfo geo_col = ExtractGeoColumnFromArg(*func_expr.children[0], schema, column_ids, 0);
	if (!geo_col.IsValid()) {
		geo_col = ExtractGeoColumnFromArg(*func_expr.children[1], schema, column_ids, 1);
	}
	if (!geo_col.IsValid()) {
		return false;
	}

	// Extract the constant geometry (must be a point for geo_distance).
	idx_t const_arg_idx = (geo_col.arg_index == 0) ? 1 : 0;
	ConstantGeoInfo const_geo = ExtractConstantGeo(*func_expr.children[const_arg_idx]);
	if (!const_geo.IsValid() || const_geo.is_envelope) {
		return false;
	}

	// Build a modified comparison expression for the ExpressionFilter.
	// Replace the constant GEOMETRY argument of ST_Distance with a VARCHAR GeoJSON string,
	// so the filter translator can reconstruct lat/lon and distance.
	auto modified_expr = comp_expr.Copy();
	auto &mod_comp = modified_expr->Cast<BoundComparisonExpression>();

	// Get the ST_Distance function expression in the modified copy.
	auto &mod_func_ref = func_on_left ? mod_comp.left : mod_comp.right;
	auto &mod_func = mod_func_ref->Cast<BoundFunctionExpression>();
	mod_func.children[const_arg_idx] = make_uniq<BoundConstantExpression>(Value(const_geo.geojson));

	ReplaceColumnRefsWithBoundRefs(modified_expr);
	auto expr_filter = make_uniq<ExpressionFilter>(std::move(modified_expr));

	get.table_filters.PushFilter(ProjectionIndex(geo_col.col_path.output_col_idx), std::move(expr_filter));
	return true;
}

// Shared pushdown complex filter implementation.
// Processes all filter expressions in a single pass:
// - Comparison filters -> ConstantFilter
// - IS NULL / IS NOT NULL -> IsNullFilter / IsNotNullFilter
// - IN expressions -> InFilter
// - LIKE/ILIKE patterns, prefix/suffix/contains -> ExpressionFilter
// - ST_Distance comparisons -> ExpressionFilter
// - ST_DWithin, ST_Within, ST_Intersects, ST_Contains, ST_Disjoint -> ExpressionFilter
//
// All recognized filters are pushed into get.table_filters and consumed from the filters vector.
// This approach handles everything in one pass, avoiding the issue where pushing to table_filters
// in pushdown_complex_filter causes DuckDB's optimizer to skip the FilterCombiner path,
// which would prevent standard comparison filters from being pushed down.
//
// For text fields without a .keyword subfield, comparison/LIKE/ILIKE/IN filters are not
// pushed - they are left for DuckDB's FILTER stage. For geo fields (geo_point, geo_shape),
// equality/inequality and IN are deferred to DuckDB's FILTER stage, while range comparisons
// are rejected with an error. When such filters are deferred and no other filters have been
// pushed into table_filters, a no-op IsNotNullFilter on _id is injected as a guard.
// This causes DuckDB's FilterCombiner (which runs after this callback) to see a non-empty
// table_filters and skip its own pushdown, preventing it from re-pushing the deferred filters
// as ConstantFilter/InFilter. The guard is optimized away by the optimizer extension
// (OptimizeIdFilters in elasticsearch_optimizer.cpp) as part of the general _id semantic
// optimization: _id IS NOT NULL is always true, so it is stripped. TranslateFilters also
// skips _id null filters as defense-in-depth.
void PushdownComplexFilters(ClientContext &context, LogicalGet &get, const ElasticsearchPushdownContext &pushdown_ctx,
                            vector<unique_ptr<Expression>> &filters) {
	const auto &schema = pushdown_ctx.schema;
	const auto &column_ids = get.GetColumnIds();

	// Track whether any filters were deferred to DuckDB (e.g. comparisons/IN on text fields without
	// .keyword or geo fields). Used to decide whether a guard filter is needed - see comment block
	// after the loop.
	bool has_deferred_filters = false;

	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];
		if (!filter) {
			continue;
		}

		// Handle comparison expressions.
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto &comp_expr = filter->Cast<BoundComparisonExpression>();

			// Skip comparisons on text fields without .keyword (they cannot be pushed to Elasticsearch
			// because the field is analyzed/tokenized). The guard filter mechanism (see after the loop)
			// prevents the FilterCombiner from re-pushing these as ConstantFilter.
			ColumnPathInfo col_path_info = ExtractColumnPath(*comp_expr.left, schema, column_ids);
			if (!col_path_info.IsValid()) {
				col_path_info = ExtractColumnPath(*comp_expr.right, schema, column_ids);
			}

			if (col_path_info.IsValid()) {
				const string &col_name = col_path_info.full_path;
				bool is_text_field = schema.text_fields.count(col_name) > 0;
				bool has_keyword_subfield = schema.text_fields_with_keyword.count(col_name) > 0;

				if (is_text_field && !has_keyword_subfield) {
					has_deferred_filters = true;
					continue;
				}

				// Standard comparisons on geo fields cannot be pushed to Elasticsearch:
				// - = and != would generate invalid term queries on geo fields
				// - <, >, <=, >= are semantically meaningless for geometry types
				// Note: ST_Distance(...) < N patterns are not caught here because ExtractColumnPath
				// returns invalid for function expressions like ST_Distance - they fall through to
				// TryPushGeoDistanceFilter below.
				bool is_geo_field = schema.geo_fields.count(col_name) > 0;
				if (is_geo_field) {
					auto comp_type = comp_expr.GetExpressionType();
					if (comp_type == ExpressionType::COMPARE_GREATERTHAN ||
					    comp_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
					    comp_type == ExpressionType::COMPARE_LESSTHAN ||
					    comp_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
						throw InvalidInputException(
						    "Range comparisons (<, >, <=, >=) are not supported on geo field '%s'.", col_name);
					}
					// = and != are deferred to DuckDB's FILTER stage.
					has_deferred_filters = true;
					continue;
				}
			}

			// Try geo_distance pushdown first: ST_Distance(...) < N or N > ST_Distance(...).
			if (TryPushGeoDistanceFilter(context, comp_expr, schema, column_ids, get)) {
				filters[i] = nullptr;
				continue;
			}

			// Push comparison as ConstantFilter.
			if (TryPushComparisonFilter(context, comp_expr, schema, column_ids, get)) {
				filters[i] = nullptr;
			}
			continue;
		}

		// Handle LIKE/ILIKE patterns, string functions and geospatial functions.
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &func_expr = filter->Cast<BoundFunctionExpression>();
			const auto &func_name = func_expr.function.name;

			// Handle LIKE/ILIKE patterns and optimized string functions (prefix, suffix, contains).
			// DuckDB's optimizer transforms LIKE patterns before filter pushdown:
			// - LikeOptimizationRule: LIKE 'prefix%' -> prefix(), LIKE '%suffix' -> suffix() etc.
			// - FilterCombiner: converts prefix() to range filters and returns PUSHED_DOWN_PARTIALLY
			// By intercepting here, we can use Elasticsearch's native prefix/wildcard queries.
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

				ColumnPathInfo col_path_info = ExtractColumnPath(*func_expr.children[0], schema, column_ids);
				if (!col_path_info.IsValid()) {
					continue;
				}

				const string &col_name = col_path_info.full_path;
				bool is_text_field = schema.text_fields.count(col_name) > 0;
				bool has_keyword_subfield = schema.text_fields_with_keyword.count(col_name) > 0;

				// Skip LIKE/ILIKE on text fields without .keyword (they cannot be pushed to Elasticsearch
				// because the field is analyzed/tokenized). The guard filter mechanism (see after the loop)
				// prevents the FilterCombiner from re-pushing these.
				if (is_text_field && !has_keyword_subfield) {
					has_deferred_filters = true;
					continue;
				}

				auto modified_expr = filter->Copy();
				ReplaceColumnRefsWithBoundRefs(modified_expr);
				get.table_filters.PushFilter(ProjectionIndex(col_path_info.output_col_idx),
				                             make_uniq<ExpressionFilter>(std::move(modified_expr)));
				filters[i] = nullptr;
				continue;
			}

			// Handle geospatial functions from the spatial extension.
			string func_name_lower = StringUtil::Lower(func_name);
			if (func_name_lower == "st_within" || func_name_lower == "st_intersects" ||
			    func_name_lower == "st_contains" || func_name_lower == "st_disjoint") {
				if (func_expr.children.size() < 2) {
					continue;
				}

				// Try to find the geo column in either argument position.
				GeoColumnInfo geo_col = ExtractGeoColumnFromArg(*func_expr.children[0], schema, column_ids, 0);
				if (!geo_col.IsValid()) {
					geo_col = ExtractGeoColumnFromArg(*func_expr.children[1], schema, column_ids, 1);
				}
				if (!geo_col.IsValid()) {
					continue;
				}

				// The other argument must be a constant geometry expression.
				idx_t const_arg_idx = (geo_col.arg_index == 0) ? 1 : 0;
				ConstantGeoInfo const_geo = ExtractConstantGeo(*func_expr.children[const_arg_idx]);
				if (!const_geo.IsValid()) {
					continue;
				}

				// Build a modified expression copy where the constant GEOMETRY argument
				// is replaced with a VARCHAR GeoJSON string.
				auto modified_expr = filter->Copy();
				auto &mod_func = modified_expr->Cast<BoundFunctionExpression>();

				if (const_geo.geojson.empty() && const_geo.is_envelope) {
					string envelope_geojson = "{\"type\":\"envelope\",\"coordinates\":[[" + to_string(const_geo.xmin) +
					                          "," + to_string(const_geo.ymax) + "],[" + to_string(const_geo.xmax) +
					                          "," + to_string(const_geo.ymin) + "]]}";
					mod_func.children[const_arg_idx] =
					    make_uniq<BoundConstantExpression>(Value(std::move(envelope_geojson)));
				} else if (!const_geo.geojson.empty()) {
					mod_func.children[const_arg_idx] = make_uniq<BoundConstantExpression>(Value(const_geo.geojson));
				}

				ReplaceColumnRefsWithBoundRefs(modified_expr);
				auto expr_filter = make_uniq<ExpressionFilter>(std::move(modified_expr));

				get.table_filters.PushFilter(ProjectionIndex(geo_col.col_path.output_col_idx), std::move(expr_filter));
				filters[i] = nullptr;
				continue;
			}

			// Handle ST_DWithin(geom1, geom2, distance) -> geo_distance query.
			// ST_DWithin is a 3-args function: two geometry arguments and a numeric distance.
			// However, the spatial extension's Bind function constant-folds the distance argument.
			// When the distance is a foldable constant (which is the common case), it erases
			// children[2] and stores the distance in bind_info. So the expression may arrive
			// with either 2 or 3 children.
			if (func_name_lower == "st_dwithin") {
				// Try to find the geo column in arg 0 or arg 1.
				GeoColumnInfo geo_col = ExtractGeoColumnFromArg(*func_expr.children[0], schema, column_ids, 0);
				if (!geo_col.IsValid()) {
					geo_col = ExtractGeoColumnFromArg(*func_expr.children[1], schema, column_ids, 1);
				}
				if (!geo_col.IsValid()) {
					continue;
				}

				// The other geometry argument must be a constant (the reference point).
				idx_t const_arg_idx = (geo_col.arg_index == 0) ? 1 : 0;
				ConstantGeoInfo const_geo = ExtractConstantGeo(*func_expr.children[const_arg_idx]);
				if (!const_geo.IsValid() || const_geo.is_envelope) {
					continue;
				}

				// Extract the distance. Two cases:
				// - 3 children: distance is in children[2] (spatial extension didn't constant-fold it)
				// - 2 children: distance was erased by the spatial extension's Bind and stored in bind_info
				double distance_meters = 0;
				bool distance_found = false;

				if (func_expr.children.size() == 3) {
					// Case 1: distance is still in the expression as children[2].
					if (!func_expr.children[2]->IsFoldable()) {
						continue;
					}
					Value dist_val;
					if (!ExpressionExecutor::TryEvaluateScalar(context, *func_expr.children[2], dist_val)) {
						continue;
					}
					if (dist_val.IsNull() || !dist_val.DefaultTryCastAs(LogicalType::DOUBLE)) {
						continue;
					}
					distance_meters = DoubleValue::Get(dist_val);
					distance_found = true;
				} else if (func_expr.bind_info) {
					// Case 2: the spatial extension erased the distance argument at bind time.
					// The distance is stored in bind_info with a layout compatible with
					// SpatialDWithinBindData (double distance as the first data member).
					auto *dwithin_bind = reinterpret_cast<const SpatialDWithinBindData *>(func_expr.bind_info.get());
					distance_meters = dwithin_bind->distance;
					distance_found = true;
				}

				if (!distance_found || distance_meters < 0) {
					continue;
				}

				// Build a modified expression with the GEOMETRY constant replaced by a GeoJSON string.
				// If the distance was erased from children, re-add it so that the filter translator
				// (TranslateGeoDistanceDWithin) always sees 3 children.
				auto modified_expr = filter->Copy();
				auto &mod_func = modified_expr->Cast<BoundFunctionExpression>();
				mod_func.children[const_arg_idx] = make_uniq<BoundConstantExpression>(Value(const_geo.geojson));
				if (mod_func.children.size() < 3) {
					mod_func.children.push_back(make_uniq<BoundConstantExpression>(Value::DOUBLE(distance_meters)));
				}

				ReplaceColumnRefsWithBoundRefs(modified_expr);
				auto expr_filter = make_uniq<ExpressionFilter>(std::move(modified_expr));

				get.table_filters.PushFilter(ProjectionIndex(geo_col.col_path.output_col_idx), std::move(expr_filter));
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

				ColumnPathInfo col_path_info = ExtractColumnPath(*op_expr.children[0], schema, column_ids);
				if (!col_path_info.IsValid()) {
					continue;
				}

				auto column_side = op_expr.children[0]->Copy();
				ReplaceColumnRefsWithBoundRefs(column_side);
				auto null_check = ExpressionFilter::CreateNullCheckExpression(std::move(column_side), expr_type);

				get.table_filters.PushFilter(ProjectionIndex(col_path_info.output_col_idx),
				                             make_uniq<ExpressionFilter>(std::move(null_check)));
				filters[i] = nullptr;
				continue;
			}

			// IN expressions
			if (expr_type == ExpressionType::COMPARE_IN) {
				if (op_expr.children.size() < 2) {
					continue;
				}

				ColumnPathInfo col_path_info = ExtractColumnPath(*op_expr.children[0], schema, column_ids);
				if (!col_path_info.IsValid()) {
					continue;
				}

				const string &col_name = col_path_info.full_path;

				bool is_text_field = schema.text_fields.count(col_name) > 0;
				bool has_keyword_subfield = schema.text_fields_with_keyword.count(col_name) > 0;

				// Skip IN on text fields without .keyword (they cannot be pushed to Elasticsearch
				// because the field is analyzed/tokenized). The guard filter mechanism (see after the loop)
				// prevents the FilterCombiner from re-pushing these.
				if (is_text_field && !has_keyword_subfield) {
					has_deferred_filters = true;
					continue;
				}

				// Skip IN on geo fields (term/terms queries are invalid on geo_point/geo_shape).
				// The guard filter mechanism prevents the FilterCombiner from re-pushing these.
				if (schema.geo_fields.count(col_name) > 0) {
					has_deferred_filters = true;
					continue;
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

				auto column_side = op_expr.children[0]->Copy();
				ReplaceColumnRefsWithBoundRefs(column_side);
				auto in_expr = ExpressionFilter::CreateInExpression(std::move(column_side), std::move(in_values));

				get.table_filters.PushFilter(ProjectionIndex(col_path_info.output_col_idx),
				                             make_uniq<ExpressionFilter>(std::move(in_expr)));
				filters[i] = nullptr;
				continue;
			}
		}
	}

	// Guard filter: prevent the FilterCombiner from re-pushing deferred filters.
	// DuckDB's optimizer runs two filter pushdown stages:
	// 1. pushdown_complex_filter (this callback) - we selectively push supported filters.
	// 2. FilterCombiner - runs only if table_filters is empty after stage 1.
	//    It would convert leftover comparison expressions into ConstantFilter/InFilter, which
	//    would then be pushed to Elasticsearch and produce incorrect results (e.g. term/range
	//    queries on analyzed text fields or geo fields).
	//
	// To prevent stage 2 from running, we ensure table_filters is never empty when there are
	// deferred filters. We push a no-op IsNotNullFilter on _id (which is always non-null
	// in Elasticsearch). If _id is not already in the projected columns, we add it via AddColumnId -
	// DuckDB's filter_prune ensures the extra column won't appear in the query output.
	//
	// The guard is optimized away by the optimizer extension (OptimizeIdFilters in
	// elasticsearch_optimizer.cpp) as part of the _id semantic optimization: since _id is always
	// non-null, "_id IS NOT NULL" is always true and gets stripped. This happens after the
	// FILTER_PUSHDOWN pass but before physical plan creation, so the guard never appears in
	// EXPLAIN output. TranslateFilters also skips _id null filters as defense-in-depth.
	if (has_deferred_filters && !get.table_filters.HasFilters()) {
		// Find _id in column_ids. For elasticsearch_query, _id is schema column 0.
		// For elasticsearch_aggregate, _id is virtual column VIRTUAL_COLUMN_START.
		// If _id is not already projected (e.g. SELECT count(*)), add it - DuckDB's filter_prune
		// ensures the extra column won't appear in the query output.
		optional_idx guard_proj_idx;
		for (idx_t ci = 0; ci < column_ids.size(); ci++) {
			idx_t primary = column_ids[ci].GetPrimaryIndex();
			if (primary == 0 || primary == VIRTUAL_COLUMN_START) {
				guard_proj_idx = ci;
				break;
			}
		}
		if (!guard_proj_idx.IsValid()) {
			// Determine the correct _id column ID based on whether any existing column is virtual.
			idx_t id_col_id = 0;
			if (!column_ids.empty() && column_ids[0].GetPrimaryIndex() >= VIRTUAL_COLUMN_START) {
				id_col_id = VIRTUAL_COLUMN_START;
			}
			guard_proj_idx = get.AddColumnId(id_col_id).GetIndex();
		}
		get.table_filters.PushFilter(
		    ProjectionIndex(guard_proj_idx.GetIndex()),
		    make_uniq<ExpressionFilter>(ExpressionFilter::CreateNullCheckExpression(
		        make_uniq<BoundReferenceExpression>("_id", LogicalType::VARCHAR, 0ULL),
		        ExpressionType::OPERATOR_IS_NOT_NULL)));
	}

	// Remove processed filters.
	filters.erase(
	    std::remove_if(filters.begin(), filters.end(), [](const unique_ptr<Expression> &e) { return e == nullptr; }),
	    filters.end());
}

} // namespace duckdb
