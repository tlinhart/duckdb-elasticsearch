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

// Translates DuckDB TableFilter objects into Elasticsearch Query DSL.
// Returns a yyjson_mut_val representing the Elasticsearch query, or nullptr if no filters.
// The caller owns the returned value (it's part of the provided doc).
//
// Parameters:
//   doc: The mutable JSON document to create values in.
//   filters: The DuckDB TableFilterSet containing pushed filters.
//   column_names: The column names corresponding to filter column indices.
//   es_types: Map from column name to Elasticsearch field type (for .keyword handling).
//   text_fields: Set of field names that are text type (need .keyword for exact match).
yyjson_mut_val *TranslateFilters(yyjson_mut_doc *doc, const TableFilterSet &filters, const vector<string> &column_names,
                                 const unordered_map<string, string> &es_types,
                                 const unordered_set<string> &text_fields);

} // namespace duckdb
