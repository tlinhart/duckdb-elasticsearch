#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "yyjson.hpp"

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace duckdb {

using namespace duckdb_yyjson;

// Translates DuckDB TableFilter objects into Elasticsearch Query DSL.
// This class handles the conversion of SQL WHERE clauses to Elasticsearch queries.
class ElasticsearchFilterTranslator {
public:
	// Main entry point that translates DuckDB filters to Elasticsearch query DSL.
	// Returns a yyjson_mut_val representing the Elasticsearch query, or nullptr if no filters.
	// The caller owns the returned value (it's part of the provided doc).
	//
	// Parameters:
	//   doc: The mutable JSON document to create values in.
	//   filters: The DuckDB TableFilterSet containing pushed filters.
	//   column_names: The column names corresponding to filter column indices.
	//   es_types: Map from column name to Elasticsearch field type (for .keyword handling).
	//   text_fields: Set of field names that are text type (need .keyword for exact match).
	static yyjson_mut_val *TranslateFilters(yyjson_mut_doc *doc, const TableFilterSet &filters,
	                                        const vector<string> &column_names,
	                                        const unordered_map<string, string> &es_types,
	                                        const unordered_set<string> &text_fields);

	// Translate a single filter for a specific column.
	// The es_types map contains field name to Elasticsearch type mappings.
	// The text_fields set contains field names that are text type (need .keyword for exact match).
	// These maps support nested field paths.
	static yyjson_mut_val *TranslateFilter(yyjson_mut_doc *doc, const TableFilter &filter, const string &column_name,
	                                       const unordered_map<string, string> &es_types,
	                                       const unordered_set<string> &text_fields);

private:
	// Individual filter translators.
	static yyjson_mut_val *TranslateConstantComparison(yyjson_mut_doc *doc, const ConstantFilter &filter,
	                                                   const string &field_name, bool is_text_field);

	static yyjson_mut_val *TranslateIsNull(yyjson_mut_doc *doc, const string &field_name);

	static yyjson_mut_val *TranslateIsNotNull(yyjson_mut_doc *doc, const string &field_name);

	static yyjson_mut_val *TranslateConjunctionAnd(yyjson_mut_doc *doc, const ConjunctionAndFilter &filter,
	                                               const string &column_name,
	                                               const unordered_map<string, string> &es_types,
	                                               const unordered_set<string> &text_fields);

	static yyjson_mut_val *TranslateConjunctionOr(yyjson_mut_doc *doc, const ConjunctionOrFilter &filter,
	                                              const string &column_name,
	                                              const unordered_map<string, string> &es_types,
	                                              const unordered_set<string> &text_fields);

	static yyjson_mut_val *TranslateInFilter(yyjson_mut_doc *doc, const InFilter &filter, const string &field_name,
	                                         bool is_text_field);

	static yyjson_mut_val *TranslateExpressionFilter(yyjson_mut_doc *doc, const ExpressionFilter &filter,
	                                                 const string &column_name, bool is_text_field);

	// Helper to translate LIKE pattern to Elasticsearch wildcard query.
	// Returns nullptr if pattern cannot be translated.
	static yyjson_mut_val *TranslateLikePattern(yyjson_mut_doc *doc, const string &field_name, const string &pattern,
	                                            bool is_text_field);

	// Helper to get the field name for Elasticsearch query (adds .keyword suffix for text fields).
	static string GetFieldName(const string &column_name, bool is_text_field);

	// Helper to convert DuckDB Value to yyjson_mut_val.
	static yyjson_mut_val *ValueToJson(yyjson_mut_doc *doc, const Value &value);
};

} // namespace duckdb
