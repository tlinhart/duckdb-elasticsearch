#define DUCKDB_EXTENSION_MAIN

#include "elasticsearch_extension.hpp"
#include "elasticsearch_query.hpp"
#include "elasticsearch_optimizer.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register table functions.
	RegisterElasticsearchQueryFunction(loader);

	// Register optimizer extension for LIMIT/OFFSET pushdown.
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.optimizer_extensions.push_back(ElasticsearchOptimizerExtension());
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
