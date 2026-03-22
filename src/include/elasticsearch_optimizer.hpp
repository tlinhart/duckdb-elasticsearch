#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

// Optimizer extension for Elasticsearch plan rewriting.
// Performs two transformations on the logical plan after all built-in optimizer passes:
// 1. _id field semantic optimization - in Elasticsearch, the _id metadata field is always
//    non-null (every document has an _id). This allows compile-time optimization:
//      - _id IS NOT NULL  ->  always true   ->  filter stripped (no-op)
//      - _id IS NULL      ->  always false  ->  scan replaced with EMPTY_RESULT
//    This applies to both elasticsearch_query (column 0) and elasticsearch_aggregate
//    (virtual column VIRTUAL_COLUMN_START). It also serves as the mechanism for stripping
//    the internal guard filter that the pushdown_complex_filter callback pushes to block the
//    FilterCombiner from re-pushing deferred filters (text fields without .keyword,
//    comparison/IN on geo fields). The guard is semantically "_id IS NOT NULL" and gets
//    optimized away as part of the always-true case.
// 2. LIMIT/OFFSET pushdown - finds LIMIT operators above Elasticsearch scans, stores the
//    limit and offset values in the bind data and removes the LIMIT operator from the plan
//    so that DuckDB does not duplicate limit enforcement. Only applies to elasticsearch_query.
class ElasticsearchOptimizerExtension : public OptimizerExtension {
public:
	ElasticsearchOptimizerExtension();
};

} // namespace duckdb
