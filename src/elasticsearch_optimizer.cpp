#include "elasticsearch_optimizer.hpp"
#include "elasticsearch_query.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"

namespace duckdb {

// In Elasticsearch, the _id metadata field is always non-null: every document has an _id.
// This lets us optimize filters on _id at compile time:
//
//   _id IS NOT NULL  ->  always true   ->  strip the filter (no-op)
//   _id IS NULL      ->  always false  ->  replace the scan with EMPTY_RESULT
//
// This also serves as the mechanism for stripping the internal guard filter. The
// pushdown_complex_filter callback pushes an IsNotNullFilter on _id to prevent DuckDB's
// FilterCombiner from incorrectly pushing comparison filters on text fields without a
// .keyword subfield. That guard is semantically "_id IS NOT NULL" and gets stripped here
// as part of the general always-true optimization - not as a special case.
//
// Runs after the FILTER_PUSHDOWN pass but before physical plan creation, so the
// optimizations are reflected in EXPLAIN / EXPLAIN ANALYZE output.
static void OptimizeIdFilters(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		if (get.function.name == "elasticsearch_query" && get.table_filters.HasFilters()) {
			// Find the projection index that maps to schema column 0 (_id).
			auto &column_ids = get.GetColumnIds();
			for (idx_t ci = 0; ci < column_ids.size(); ci++) {
				if (column_ids[ci].GetPrimaryIndex() != 0) {
					continue;
				}
				// Found _id at projection index ci.
				ProjectionIndex proj_idx(ci);
				auto filter = get.table_filters.TryGetFilterByColumnIndex(proj_idx);
				if (!filter) {
					break;
				}

				if (filter->filter_type == TableFilterType::IS_NOT_NULL) {
					// _id IS NOT NULL is always true -> strip (no-op).
					get.table_filters.RemoveFilterByColumnIndex(proj_idx);
				} else if (filter->filter_type == TableFilterType::IS_NULL) {
					// _id IS NULL is always false -> replace scan with empty result.
					// LogicalEmptyResult preserves column bindings and types from the
					// original node. DuckDB's EmptyResultPullup pass will propagate the
					// empty result upward through parent operators (FILTER, PROJECTION
					// etc.) so compound conditions like "_id IS NULL AND x > 5" also
					// resolve to EMPTY_RESULT.
					op = make_uniq<LogicalEmptyResult>(std::move(op));
					return; // node replaced, no children to recurse into
				}
				break;
			}
		}
	}

	for (auto &child : op->children) {
		OptimizeIdFilters(child);
	}
}

// Walks the plan tree looking for LIMIT operators directly above an elasticsearch_query scan
// (with optional intermediate PROJECTION nodes). When found, the constant limit and offset
// values are stored in the bind data and the LIMIT operator is removed from the plan so that
// DuckDB does not duplicate limit enforcement.
static void OptimizeLimitPushdown(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit_op = op->Cast<LogicalLimit>();

		// Walk through projections to find the underlying GET operator.
		// The pattern we're looking for is LIMIT -> PROJECTION* -> GET.
		reference<LogicalOperator> child_ref = *op->children[0];
		while (child_ref.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			if (child_ref.get().children.empty()) {
				break;
			}
			child_ref = *child_ref.get().children[0];
		}

		if (child_ref.get().type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = child_ref.get().Cast<LogicalGet>();

			// Check if this is an elasticsearch_query table function.
			if (get.function.name == "elasticsearch_query" && get.bind_data) {
				// Check if we can extract constant limit and offset values.
				int64_t limit_value = -1;
				int64_t offset_value = 0;
				bool can_pushdown = true;

				// Extract limit value if it's a constant.
				if (limit_op.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
					limit_value = static_cast<int64_t>(limit_op.limit_val.GetConstantValue());
				} else if (limit_op.limit_val.Type() != LimitNodeType::UNSET) {
					// Non-constant limit (expression or percentage), cannot push down.
					can_pushdown = false;
				}

				// Extract offset value if it's a constant.
				if (limit_op.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
					offset_value = static_cast<int64_t>(limit_op.offset_val.GetConstantValue());
				} else if (limit_op.offset_val.Type() != LimitNodeType::UNSET) {
					// Non-constant offset (expression or percentage), cannot push down.
					can_pushdown = false;
				}

				if (can_pushdown && (limit_value > 0 || offset_value > 0)) {
					// Store limit and offset in the bind data.
					SetElasticsearchLimitOffset(*get.bind_data, limit_value, offset_value);

					// Remove the LIMIT operator from the plan since we're handling it.
					op = std::move(op->children[0]);

					// Continue optimizing the new root (which was the child).
					OptimizeLimitPushdown(op);
					return;
				}
			}
		}

		// Could not push down, recurse into children.
		for (auto &child : op->children) {
			OptimizeLimitPushdown(child);
		}
		return;
	}

	// For all other operator types, recurse into children.
	for (auto &child : op->children) {
		OptimizeLimitPushdown(child);
	}
}

void OptimizeElasticsearchPlan(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	OptimizeIdFilters(plan);
	OptimizeLimitPushdown(plan);
}

ElasticsearchOptimizerExtension::ElasticsearchOptimizerExtension() {
	optimize_function = OptimizeElasticsearchPlan;
}

} // namespace duckdb
