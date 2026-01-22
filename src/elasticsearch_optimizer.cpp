#include "elasticsearch_optimizer.hpp"
#include "elasticsearch_query.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

namespace duckdb {

// Recursively optimize the plan tree for LIMIT pushdown.
static void OptimizeLimitPushdownRecursive(unique_ptr<LogicalOperator> &op) {
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
					OptimizeLimitPushdownRecursive(op);
					return;
				}
			}
		}

		// Could not push down, recurse into children.
		for (auto &child : op->children) {
			OptimizeLimitPushdownRecursive(child);
		}
		return;
	}

	// For all other operator types, recurse into children.
	for (auto &child : op->children) {
		OptimizeLimitPushdownRecursive(child);
	}
}

void OptimizeElasticsearchLimitPushdown(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	OptimizeLimitPushdownRecursive(plan);
}

ElasticsearchOptimizerExtension::ElasticsearchOptimizerExtension() {
	optimize_function = OptimizeElasticsearchLimitPushdown;
}

} // namespace duckdb
