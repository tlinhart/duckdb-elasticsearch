#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "elasticsearch_schema.hpp"
#include "yyjson.hpp"

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
//   column_names: The column names corresponding to filter column indices (built from column_ids,
//                 not necessarily the same as schema.column_names).
//   schema: The resolved Elasticsearch schema containing type maps and text field information
//           needed for filter translation (.keyword handling, type lookups).
FilterTranslationResult TranslateFilters(yyjson_mut_doc *doc, const TableFilterSet &filters,
                                         const vector<string> &column_names, const ElasticsearchSchema &schema);

} // namespace duckdb
