#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

// Optimizer extension for Elasticsearch LIMIT/OFFSET pushdown.
// This extension walks the logical plan to find LIMIT operators above Elasticsearch scans,
// extracts the limit and offset values, stores them in the bind data, and removes the
// LIMIT operator from the plan so that DuckDB does not duplicate limit enforcement.
class ElasticsearchOptimizerExtension : public OptimizerExtension {
public:
	ElasticsearchOptimizerExtension();
};

// The main optimization function that performs LIMIT pushdown.
// Recursively walks the logical plan tree looking for LIMIT operators above Elasticsearch scans.
void OptimizeElasticsearchLimitPushdown(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
