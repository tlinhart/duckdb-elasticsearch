#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "es_client.hpp"

namespace duckdb {

// Register the es_aggregate table function.
void RegisterElasticsearchAggregateFunction(ExtensionLoader &loader);

} // namespace duckdb
