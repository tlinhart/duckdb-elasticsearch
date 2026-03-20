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
// Filters on text fields without a .keyword subfield (other than IS NULL / IS NOT NULL)
// are prevented from reaching this function by the guard filter mechanism in
// pushdown_complex_filter. A no-op IsNotNullFilter on _id gates off DuckDB's FilterCombiner,
// preventing it from pushing ConstantFilter/InFilter for those columns. The guard is optimized
// away by the optimizer extension (OptimizeIdFilters in elasticsearch_optimizer.cpp) as part of
// the _id semantic optimization before physical plan creation. TranslateFilters also skips _id
// null filters as defense-in-depth. Additional nullptr guards in the individual translate
// functions provide a further safety net.
//
// Parameters:
//   doc: The mutable JSON document to create values in.
//   filters: The DuckDB TableFilterSet containing pushed filters.
//   column_names: The column names corresponding to filter column indices (built from column_ids,
//                 not necessarily the same as schema.column_names).
//   schema: The resolved Elasticsearch schema containing type maps and text field information
//           needed for filter translation (.keyword handling, field type checks).
FilterTranslationResult TranslateFilters(yyjson_mut_doc *doc, const TableFilterSet &filters,
                                         const vector<string> &column_names, const ElasticsearchSchema &schema);

} // namespace duckdb
