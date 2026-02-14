#define DUCKDB_EXTENSION_MAIN

#include "elasticsearch_extension.hpp"
#include "elasticsearch_cache.hpp"
#include "elasticsearch_query.hpp"
#include "elasticsearch_optimizer.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"

// Detect DuckDB v1.5+ by checking for the new extension_callback_manager header
// introduced in PR #20599. If __has_include is unsupported or the header is
// missing, we fall back to the old API (safe for older DuckDB versions).
#if defined(__has_include)
#if __has_include(<duckdb/main/extension_callback_manager.hpp>)
#define DUCKDB_V1_5_OR_LATER true
#endif
#endif

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register table functions.
	RegisterElasticsearchQueryFunction(loader);

	// Register scalar functions.
	RegisterElasticsearchClearCacheFunction(loader);

	// Register optimizer extension for LIMIT/OFFSET pushdown.
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
#ifdef DUCKDB_V1_5_OR_LATER
	OptimizerExtension::Register(config, ElasticsearchOptimizerExtension());
#else
	config.optimizer_extensions.push_back(ElasticsearchOptimizerExtension());
#endif
}

void ElasticsearchExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string ElasticsearchExtension::Name() {
	return "elasticsearch";
}

std::string ElasticsearchExtension::Version() const {
#ifdef EXT_VERSION_ELASTICSEARCH
	return EXT_VERSION_ELASTICSEARCH;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(elasticsearch, loader) {
	duckdb::LoadInternal(loader);
}
}
