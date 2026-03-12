#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Register the elasticsearch_query table function.
void RegisterElasticsearchQueryFunction(ExtensionLoader &loader);

// Helper function for optimizer extension to set limit/offset pushdown values in bind data.
void SetElasticsearchLimitOffset(FunctionData &bind_data, int64_t limit, int64_t offset);

} // namespace duckdb
