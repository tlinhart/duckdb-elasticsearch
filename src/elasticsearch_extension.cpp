#define DUCKDB_EXTENSION_MAIN

#include "elasticsearch_extension.hpp"
#include "elasticsearch_query.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register table functions.
	RegisterElasticsearchQueryFunction(loader);
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
