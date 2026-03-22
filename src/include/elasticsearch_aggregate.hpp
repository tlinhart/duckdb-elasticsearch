#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Register the elasticsearch_aggregate table function.
void RegisterElasticsearchAggregateFunction(ExtensionLoader &loader);

} // namespace duckdb
