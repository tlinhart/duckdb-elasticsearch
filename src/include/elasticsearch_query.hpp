#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "elasticsearch_client.hpp"

namespace duckdb {

// Register the elasticsearch_query table function with filter/projection pushdown support.
void RegisterElasticsearchQueryFunction(ExtensionLoader &loader);

} // namespace duckdb
