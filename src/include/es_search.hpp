#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "es_client.hpp"

namespace duckdb {

// Register the es_search table function.
void RegisterElasticsearchSearchFunction(ExtensionLoader &loader);

} // namespace duckdb
