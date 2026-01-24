#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "yyjson.hpp"

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace duckdb {

using namespace duckdb_yyjson;

// Result of filter translation containing the Elasticsearch query.
struct FilterTranslationResult {
	// The translated Elasticsearch query (nullptr if no filters were translated).
	yyjson_mut_val *es_query;
};

// Translates DuckDB TableFilter objects into Elasticsearch Query DSL.
// Returns a FilterTranslationResult containing the translated Elasticsearch query (nullptr if no filters).
//
// Important: For text fields without a .keyword subfield, only IS NULL / IS NOT NULL filters are supported.
// Any other filter type on such fields will throw an InvalidInputException with a helpful error message
// explaining workarounds.
//
// Parameters:
//   doc: The mutable JSON document to create values in.
//   filters: The DuckDB TableFilterSet containing pushed filters.
//   column_names: The column names corresponding to filter column indices.
//   es_types: Map from column name to Elasticsearch field type (for .keyword handling).
//   text_fields: Set of field names that are text type (need .keyword for exact match).
//   text_fields_with_keyword: Set of text fields that have a .keyword subfield.
FilterTranslationResult TranslateFilters(yyjson_mut_doc *doc, const TableFilterSet &filters,
                                         const vector<string> &column_names,
                                         const unordered_map<string, string> &es_types,
                                         const unordered_set<string> &text_fields,
                                         const unordered_set<string> &text_fields_with_keyword);

} // namespace duckdb
