# DuckDB Elasticsearch Extension

A DuckDB extension that enables querying Elasticsearch indices directly using
SQL. Bring the power of SQL analytics to your Elasticsearch data without ETL
pipelines or data movement.

## Overview

This extension provides a table function that allows you to:

- Query Elasticsearch indices using familiar SQL syntax.
- Leverage DuckDB's query optimizer with filter, projection and limit pushdown.
- Join Elasticsearch data with local tables, Parquet files or other data
  sources.

The extension automatically infers the schema from Elasticsearch index
mappings, handles type conversions and supports advanced features like nested
objects, geo types, and multi-index queries.

## Features

### Query optimization

- Filter pushdown – `WHERE` clauses are automatically translated to
  Elasticsearch Query DSL and executed server-side, reducing data transfer.
- Projection pushdown – only requested columns are fetched via `_source`
  filtering.
- Limit pushdown – `LIMIT` and `OFFSET` clauses are pushed to Elasticsearch via
  an optimizer extension.

### Automatic schema inference

- Schema is inferred from Elasticsearch index mappings at query time.
- Supports multi-index queries (e.g. `logs-*`) with automatic mapping merging.
- Array fields are detected by sampling documents.
- Unmapped/dynamic fields are collected into a JSON column.

### Type support

- Full support for Elasticsearch scalar types (`text`, `keyword`, `integer`,
  `float`, `date`, `boolean`, `ip` etc.)
- Nested objects mapped to DuckDB `STRUCT` types.
- Nested arrays mapped to `LIST(STRUCT(...))` types.
- Geo types (`geo_point`, `geo_shape`) converted to GeoJSON format.
- WKT geometry strings automatically parsed and converted.

### Reliability

- Scroll API for efficient retrieval of large result sets.
- Automatic retry with exponential backoff for transient errors.
- Configurable timeouts and retry parameters.
- SSL/TLS support with optional certificate verification.

## Installation

The easiest way to install the Elasticsearch extension is from the DuckDB
[community extensions](https://duckdb.org/community_extensions) repository:

```sql
INSTALL elasticsearch FROM community;
LOAD elasticsearch;
```

## Build from source

### Prerequisites

- C++11 compatible compiler
- CMake 3.5 or higher
- [vcpkg](https://vcpkg.io) (for dependency management)
- [Ninja](https://ninja-build.org) (recommended for parallelizing the build
  process)
- [ccache](https://ccache.dev) (recommended for caching compilation results and
  faster rebuilds)

### Clone the repository

```shell
git clone --recurse-submodules https://github.com/tlinhart/duckdb-elasticsearch.git
cd duckdb-elasticsearch
```

The `--recurse-submodules` flag is required to pull the DuckDB core and
extension CI tools submodules. If you already cloned without submodules:

```shell
git submodule update --init --recursive
```

### Set up vcpkg

This DuckDB extension uses vcpkg for external dependencies management. Set it
up as follows:

```shell
git clone https://github.com/microsoft/vcpkg.git
cd vcpkg
git checkout ce613c41372b23b1f51333815feb3edd87ef8a8b
./bootstrap-vcpkg.sh -disableMetrics
export VCPKG_TOOLCHAIN_PATH=$(pwd)/scripts/buildsystems/vcpkg.cmake
```

The build system will automatically use vcpkg when `VCPKG_TOOLCHAIN_PATH` is
set. Dependencies are declared in `vcpkg.json`.

### Build with Make

The simplest way to build the extension is using Make:

```shell
make
```

This will create a release build of both the static and loadable extension.

DuckDB extensions build DuckDB itself as part of the process to provide easy
testing and distributing. To speed up rebuilds significantly it's highly
recommended to install Ninja and ccache. The build system automatically detects
and uses ccache to cache build artifacts. To parallelize builds using Ninja:

```shell
GEN=ninja make
```

To limit the number of parallel jobs (if running low on memory):

```shell
CMAKE_BUILD_PARALLEL_LEVEL=4 GEN=ninja make
```

The main binaries produced by the build are:

- `build/release/duckdb` – DuckDB shell with extension pre-loaded.
- `build/release/test/unittest` – test runner with extension linked into the
  binary.
- `build/release/extension/elasticsearch/elasticsearch.duckdb_extension` –
  loadable extension binary as it would be distributed.

### Run the tests

The Elasticsearch extension is equipped with a comprehensive test suite under
the `test` directory. To run tests after the build:

```shell
make test
```

### Load the extension

To run the extension code, simply start the built shell with pre-loaded
extension:

```shell
./build/release/duckdb
```

Alternatively, start the DuckDB shell with `-unsigned` flag and load the
extension manually:

```sql
LOAD 'build/release/extension/elasticsearch/elasticsearch.duckdb_extension';
```
