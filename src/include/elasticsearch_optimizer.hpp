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
//    This also serves as the mechanism for stripping the internal guard filter that
//    ElasticsearchPushdownComplexFilter pushes to block the FilterCombiner. The guard is
//    semantically "_id IS NOT NULL" and gets optimized away as part of the always-true case.
// 2. LIMIT/OFFSET pushdown - finds LIMIT operators above Elasticsearch scans, stores the
//    limit and offset values in the bind data and removes the LIMIT operator from the plan
//    so that DuckDB does not duplicate limit enforcement.
class ElasticsearchOptimizerExtension : public OptimizerExtension {
public:
	ElasticsearchOptimizerExtension();
};

// The main optimization function that rewrites the Elasticsearch logical plan.
// Recursively walks the plan tree to optimize _id filters and push down LIMIT/OFFSET.
void OptimizeElasticsearchPlan(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
