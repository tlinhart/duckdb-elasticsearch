# DuckDB Elasticsearch Extension

A DuckDB extension that enables querying Elasticsearch indices directly using SQL. Bring the power of SQL analytics to your Elasticsearch data without ETL pipelines or data movement.

## Overview

This extension provides table functions that allow you to:

- Query Elasticsearch indices using familiar SQL syntax
- Leverage DuckDB's query optimizer with filter and projection pushdown
- Join Elasticsearch data with local tables, Parquet files, or other data sources
- Run aggregations natively in Elasticsearch and process results in DuckDB

The extension automatically infers the schema from Elasticsearch index mappings, handles type conversions, and supports advanced features like nested objects, geo types, and multi-index queries.

## Features

### Three Table Functions

| Function       | Description                                      | Pushdown Support    |
| -------------- | ------------------------------------------------ | ------------------- |
| `es_search`    | Basic search with full Query DSL support         | None                |
| `es_query`     | Optimized search with filter/projection pushdown | Filter + Projection |
| `es_aggregate` | Execute aggregation queries                      | None                |

### Query Optimization

- **Filter Pushdown** (`es_query` only): SQL `WHERE` clauses are automatically translated to Elasticsearch Query DSL and executed server-side, reducing data transfer
- **Projection Pushdown** (`es_query` only): Only requested columns are fetched via `_source` filtering
- **Limit Pushdown**: `LIMIT` clauses are pushed to Elasticsearch to avoid fetching unnecessary documents

### Automatic Schema Inference

- Schema is inferred from Elasticsearch index mappings at query time
- Supports multi-index queries (e.g., `logs-*`) with automatic mapping merging
- Array fields are detected by sampling documents (configurable)
- Unmapped/dynamic fields are collected into a JSON column

### Type Support

- Full support for Elasticsearch scalar types (text, keyword, integer, float, date, boolean, ip, etc.)
- Nested objects mapped to DuckDB `STRUCT` types
- Nested arrays mapped to `LIST(STRUCT(...))` types
- Geo types (`geo_point`, `geo_shape`) converted to GeoJSON format
- WKT geometry strings automatically parsed and converted

### Reliability

- Automatic retry with exponential backoff for transient errors (429, 5xx)
- Scroll API for efficient retrieval of large result sets
- Configurable timeouts and retry parameters
- SSL/TLS support with optional certificate verification

## Building

This extension is built using DuckDB's [extension template](https://github.com/duckdb/extension-template), which provides a batteries-included development environment with CMake integration, vcpkg for dependency management, and GitHub Actions CI/CD pipelines.

### Build Requirements

- C++11 compatible compiler (GCC, Clang, or MSVC)
- CMake 3.5 or higher
- OpenSSL development libraries
- Git (for submodules)
- Python 3 (for scripts)

**Recommended for faster builds:**

- [Ninja](https://ninja-build.org/) - automatically parallelizes the build process
- [ccache](https://ccache.dev/) - caches compilation results for faster rebuilds

### Clone the Repository

```bash
git clone --recurse-submodules https://github.com/tlinhart/duckdb-elasticsearch.git
cd duckdb-elasticsearch
```

The `--recurse-submodules` flag is required to pull the DuckDB core and extension-ci-tools submodules.

If you already cloned without submodules:

```bash
git submodule update --init --recursive
```

### Setting Up vcpkg (Dependency Management)

DuckDB extensions use [vcpkg](https://vcpkg.io/) for dependency management. If your build requires external dependencies managed by vcpkg, set it up as follows:

```bash
# Clone vcpkg (outside the extension repository)
cd /path/to/your/workspace
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg

# Bootstrap vcpkg
./bootstrap-vcpkg.sh -disableMetrics   # Linux/macOS
# or: .\bootstrap-vcpkg.bat -disableMetrics   # Windows

# Set the toolchain path environment variable
export VCPKG_TOOLCHAIN_PATH=$(pwd)/scripts/buildsystems/vcpkg.cmake
```

The build system will automatically use vcpkg when `VCPKG_TOOLCHAIN_PATH` is set. Dependencies are declared in `vcpkg.json` at the repository root.

> **Note**: vcpkg setup is only required if the extension has external dependencies that need to be managed through vcpkg. For extensions without such dependencies, you can skip this step.

### Build with Make

The simplest way to build is using the included Makefile:

```bash
make
```

This will:

1. Configure CMake with the DuckDB build system
2. Build both the static and loadable extension
3. Output the extension to `build/release/extension/elasticsearch/`

### Build Options

```bash
# Debug build
make debug

# Release build (default)
make release

# Build using Ninja for faster parallel compilation
GEN=ninja make

# Clean build artifacts
make clean

# Run tests
make test
```

### Tips for Faster Builds

DuckDB extensions build DuckDB itself as part of the process. To speed up rebuilds significantly:

1. **Install ccache** - The build system automatically detects and uses it
2. **Install Ninja** - Use `GEN=ninja make` for parallel builds
3. **Limit parallel jobs** if running low on memory: `CMAKE_BUILD_PARALLEL_LEVEL=4 GEN=ninja make`

### Manual CMake Build

```bash
mkdir -p build/release
cd build/release
cmake -DCMAKE_BUILD_TYPE=Release ../..
make -j$(nproc)
```

### Build Outputs

The main binaries produced are:

| Path                                                                   | Description                            |
| ---------------------------------------------------------------------- | -------------------------------------- |
| `build/release/duckdb`                                                 | DuckDB shell with extension pre-loaded |
| `build/release/test/unittest`                                          | Test runner with extension linked      |
| `build/release/extension/elasticsearch/elasticsearch.duckdb_extension` | Loadable extension binary              |

### Platform Support

DuckDB extensions can be built for all platforms that DuckDB supports:

| Platform    | Architecture                          | Notes               |
| ----------- | ------------------------------------- | ------------------- |
| Linux       | x86_64, ARM64                         | glibc and musl libc |
| macOS       | x86_64 (Intel), ARM64 (Apple Silicon) | macOS 12+           |
| Windows     | x86_64, ARM64                         | Windows 10+         |
| WebAssembly | wasm                                  | For browser usage   |

The GitHub Actions CI automatically builds for all supported platforms on each push to main.

## Loading the Extension

### Load from Build Directory

```sql
-- Load the extension
LOAD 'build/release/extension/elasticsearch/elasticsearch.duckdb_extension';
```

### Install Locally

```sql
-- Install from the build directory
INSTALL 'build/release/extension/elasticsearch/elasticsearch.duckdb_extension';

-- Then load in any session
LOAD elasticsearch;
```

## Table Functions

### Common Parameters

All table functions share these connection parameters:

| Parameter              | Type    | Default       | Description                                       |
| ---------------------- | ------- | ------------- | ------------------------------------------------- |
| `host`                 | VARCHAR | `'localhost'` | Elasticsearch hostname or IP address              |
| `port`                 | INTEGER | `9200`        | Elasticsearch HTTP port                           |
| `index`                | VARCHAR | _(required)_  | Index name or pattern (e.g., `'logs-*'`)          |
| `username`             | VARCHAR | `''`          | Username for HTTP Basic authentication            |
| `password`             | VARCHAR | `''`          | Password for HTTP Basic authentication            |
| `use_ssl`              | BOOLEAN | `false`       | Use HTTPS instead of HTTP                         |
| `verify_ssl`           | BOOLEAN | `true`        | Verify SSL certificates (disable for self-signed) |
| `timeout`              | INTEGER | `30000`       | Request timeout in milliseconds                   |
| `max_retries`          | INTEGER | `3`           | Maximum retry attempts for transient errors       |
| `retry_interval`       | INTEGER | `100`         | Initial retry wait time in milliseconds           |
| `retry_backoff_factor` | DOUBLE  | `2.0`         | Exponential backoff multiplier                    |

---

### `es_search`

Basic Elasticsearch search function that executes a Query DSL query and returns results.

#### Signature

```sql
es_search(
    host := 'localhost',
    port := 9200,
    index := '<index_name>',
    query := '{"query": {"match_all": {}}}',
    username := '',
    password := '',
    use_ssl := false,
    verify_ssl := true,
    timeout := 30000,
    max_retries := 3,
    retry_interval := 100,
    retry_backoff_factor := 2.0,
    sample_size := 100
)
```

#### Additional Parameters

| Parameter     | Type    | Default                          | Description                                                      |
| ------------- | ------- | -------------------------------- | ---------------------------------------------------------------- |
| `query`       | VARCHAR | `'{"query": {"match_all": {}}}'` | Elasticsearch Query DSL as JSON string                           |
| `sample_size` | INTEGER | `100`                            | Number of documents to sample for array detection (0 to disable) |

#### How It Works

1. **Bind Phase**: Fetches index mapping from Elasticsearch, infers DuckDB schema, optionally samples documents to detect array fields
2. **Scan Phase**: Executes the query using Scroll API, fetches documents in batches, converts JSON to DuckDB values

#### Examples

**Basic query - fetch all documents:**

```sql
SELECT * FROM es_search(
    host := 'localhost',
    index := 'test'
);
```

**Query with authentication:**

```sql
SELECT name, price FROM es_search(
    host := 'elasticsearch.example.com',
    port := 9243,
    index := 'products',
    username := 'elastic',
    password := 'secret',
    use_ssl := true
);
```

**Custom Query DSL:**

```sql
SELECT name, price FROM es_search(
    host := 'localhost',
    index := 'test',
    query := '{
        "query": {
            "bool": {
                "must": [
                    {"range": {"price": {"gte": 1000, "lte": 5000}}}
                ],
                "filter": [
                    {"term": {"deprecated": false}}
                ]
            }
        },
        "sort": [{"price": "desc"}]
    }'
);
```

**Expected output (using sample data):**

```
┌───────────────────┬─────────┐
│       name        │  price  │
│      varchar      │  float  │
├───────────────────┼─────────┤
│ Alice Johnson     │ 4523.75 │
│ Daniel Taylor     │ 4012.45 │
│ Olivia Davis      │ 3456.12 │
│ Emma Wilson       │ 2341.89 │
│ Sophia Martinez   │ 1567.25 │
└───────────────────┴─────────┘
```

#### Edge Cases and Gotchas

- **Empty results**: Returns an empty result set (0 rows) if no documents match
- **Invalid query**: Throws an error with the Elasticsearch error message
- **Network errors**: Automatically retries on transient errors; throws after max retries
- **Missing index**: Throws an error if the index doesn't exist
- **No filter pushdown**: All filtering happens in Elasticsearch via the `query` parameter; DuckDB `WHERE` clauses are evaluated client-side after fetching

---

### `es_query`

Optimized search function with automatic filter and projection pushdown. Use this when you want DuckDB to optimize the query execution.

#### Signature

```sql
es_query(
    host := 'localhost',
    port := 9200,
    index := '<index_name>',
    query := '',
    username := '',
    password := '',
    use_ssl := false,
    verify_ssl := true,
    timeout := 30000,
    max_retries := 3,
    retry_interval := 100,
    retry_backoff_factor := 2.0,
    sample_size := 100
)
```

#### Additional Parameters

| Parameter     | Type    | Default      | Description                                                      |
| ------------- | ------- | ------------ | ---------------------------------------------------------------- |
| `query`       | VARCHAR | `''` (empty) | Optional base Query DSL (merged with pushed filters)             |
| `sample_size` | INTEGER | `100`        | Number of documents to sample for array detection (0 to disable) |

#### How It Works

1. **Bind Phase**: Same as `es_search` - fetches mapping, infers schema
2. **Init Phase**: Receives filter and projection information from DuckDB's optimizer
3. **Query Building**: Translates SQL filters to ES Query DSL, merges with base query using `bool.must`
4. **Scan Phase**: Executes optimized query, only fetches projected columns

#### Filter Pushdown

The following SQL expressions are translated to Elasticsearch Query DSL:

| SQL Expression            | Elasticsearch Query                                       |
| ------------------------- | --------------------------------------------------------- |
| `column = value`          | `{"term": {"column": value}}`                             |
| `column != value`         | `{"bool": {"must_not": {"term": {"column": value}}}}`     |
| `column > value`          | `{"range": {"column": {"gt": value}}}`                    |
| `column >= value`         | `{"range": {"column": {"gte": value}}}`                   |
| `column < value`          | `{"range": {"column": {"lt": value}}}`                    |
| `column <= value`         | `{"range": {"column": {"lte": value}}}`                   |
| `column IS NULL`          | `{"bool": {"must_not": {"exists": {"field": "column"}}}}` |
| `column IS NOT NULL`      | `{"exists": {"field": "column"}}`                         |
| `column IN (a, b, c)`     | `{"terms": {"column": [a, b, c]}}`                        |
| `column LIKE 'prefix%'`   | `{"prefix": {"column": "prefix"}}`                        |
| `column LIKE '%pattern%'` | `{"wildcard": {"column": {"value": "*pattern*"}}}`        |

**Special handling for text fields**: Text fields automatically use the `.keyword` subfield for exact matching since analyzed text fields don't support term queries.

#### Examples

**Simple filter pushdown:**

```sql
SELECT name, amount FROM es_query(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test'
) WHERE deprecated = true;
```

The extension translates this to:

```json
{ "query": { "term": { "deprecated": true } }, "_source": ["name", "amount"] }
```

**Expected output:**

```
┌─────────────────────┬────────┐
│        name         │ amount │
│       varchar       │ int32  │
├─────────────────────┼────────┤
│ Michael Brown       │     87 │
│ James Garcia        │     63 │
│ Olivia Davis        │      8 │
│ Charlotte Anderson  │     76 │
└─────────────────────┴────────┘
```

**Range filter:**

```sql
SELECT name, price FROM es_query(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test'
) WHERE price > 7000;
```

**Expected output:**

```
┌──────────────────┬─────────┐
│       name       │  price  │
│     varchar      │  float  │
├──────────────────┼─────────┤
│ Michael Brown    │  8912.5 │
│ William Thompson │  9234.6 │
│ Benjamin Lee     │ 7821.33 │
└──────────────────┴─────────┘
```

**Combining base query with filters:**

```sql
SELECT name, price FROM es_query(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test',
    query := '{"query": {"bool": {"must": [{"exists": {"field": "employee"}}]}}}'
) WHERE price BETWEEN 2000 AND 6000;
```

The base query and SQL filters are merged using `bool.must`.

**Projection pushdown:**

```sql
-- Only 'name' and 'price' are fetched from Elasticsearch
SELECT name, price FROM es_query(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test'
) LIMIT 5;
```

#### When to Use `es_query` vs `es_search`

| Use `es_query` when...              | Use `es_search` when...                                    |
| ----------------------------------- | ---------------------------------------------------------- |
| You have SQL WHERE clauses          | You need full Query DSL control                            |
| You want automatic optimization     | You're using complex ES queries (nested, has_parent, etc.) |
| You're selecting specific columns   | You need specific ES features not supported by pushdown    |
| You want DuckDB to handle filtering | You've already optimized the ES query                      |

---

### `es_aggregate`

Execute Elasticsearch aggregation queries and return structured results.

#### Signature

```sql
es_aggregate(
    host := 'localhost',
    port := 9200,
    index := '<index_name>',
    query := '<aggregation_query>',
    username := '',
    password := '',
    use_ssl := false,
    verify_ssl := true,
    timeout := 30000,
    max_retries := 3,
    retry_interval := 100,
    retry_backoff_factor := 2.0
)
```

#### Parameters

| Parameter | Type    | Default      | Description                                    |
| --------- | ------- | ------------ | ---------------------------------------------- |
| `query`   | VARCHAR | _(required)_ | Elasticsearch aggregation query as JSON string |

**Note**: The `query` parameter is required and must contain an `aggs` or `aggregations` field.

#### How It Works

1. **Bind Phase**: Executes the aggregation query immediately to determine the result schema
2. **Schema Inference**: Analyzes the `aggregations` response to build DuckDB types (STRUCT, LIST, etc.)
3. **Scan Phase**: Returns the pre-fetched aggregation result as a single row

#### Examples

**Terms aggregation:**

```sql
SELECT * FROM es_aggregate(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test',
    query := '{
        "size": 0,
        "aggs": {
            "colors": {
                "terms": {"field": "colors"}
            }
        }
    }'
);
```

**Expected output:**

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                  aggregations                                    │
│ struct(colors struct(doc_count_error_upper_bound bigint, sum_other_doc_count ...│
├─────────────────────────────────────────────────────────────────────────────────┤
│ {'colors': {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buck...│
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Accessing nested aggregation results:**

```sql
SELECT
    aggregations.colors.buckets AS color_buckets
FROM es_aggregate(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test',
    query := '{
        "size": 0,
        "aggs": {
            "colors": {
                "terms": {"field": "colors"}
            }
        }
    }'
);
```

**Average aggregation:**

```sql
SELECT
    aggregations.avg_price.value AS average_price
FROM es_aggregate(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test',
    query := '{
        "size": 0,
        "aggs": {
            "avg_price": {
                "avg": {"field": "price"}
            }
        }
    }'
);
```

**Date histogram:**

```sql
SELECT * FROM es_aggregate(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test',
    query := '{
        "size": 0,
        "aggs": {
            "sales_over_time": {
                "date_histogram": {
                    "field": "birth_date",
                    "calendar_interval": "year"
                }
            }
        }
    }'
);
```

#### Edge Cases and Gotchas

- **Query executed at bind time**: The aggregation runs during query planning, not execution. This means the result is cached and won't reflect real-time changes if you run the same query multiple times in a transaction.
- **Single row output**: Always returns exactly one row containing the aggregations object
- **Schema inference**: Complex nested aggregations produce deeply nested STRUCT types
- **No document results**: The `size: 0` in the query is recommended to avoid fetching documents (only aggregations are returned)

---

## Type Mapping

### Elasticsearch to DuckDB Type Mapping

| Elasticsearch Type | DuckDB Type         | Notes                                            |
| ------------------ | ------------------- | ------------------------------------------------ |
| `text`             | `VARCHAR`           | Analyzed text; use `.keyword` for exact matching |
| `keyword`          | `VARCHAR`           | Not analyzed, exact values                       |
| `long`             | `BIGINT`            | 64-bit signed integer                            |
| `integer`          | `INTEGER`           | 32-bit signed integer                            |
| `short`            | `SMALLINT`          | 16-bit signed integer                            |
| `byte`             | `TINYINT`           | 8-bit signed integer                             |
| `double`           | `DOUBLE`            | 64-bit floating point                            |
| `float`            | `FLOAT`             | 32-bit floating point                            |
| `half_float`       | `FLOAT`             | 16-bit floating point (stored as FLOAT)          |
| `boolean`          | `BOOLEAN`           | true/false                                       |
| `date`             | `TIMESTAMP`         | Parsed from ISO8601 or epoch                     |
| `ip`               | `VARCHAR`           | IP addresses as strings                          |
| `geo_point`        | `VARCHAR`           | Converted to GeoJSON Point                       |
| `geo_shape`        | `VARCHAR`           | Converted to GeoJSON geometry                    |
| `object`           | `STRUCT(...)`       | Nested properties become struct fields           |
| `nested`           | `LIST(STRUCT(...))` | Always treated as array of objects               |

### Array Handling

Elasticsearch mappings don't distinguish between scalar fields and arrays. The extension detects arrays by sampling documents:

- Sample `sample_size` documents (default: 100)
- If any document has an array value for a field, wrap the type in `LIST(...)`
- Set `sample_size := 0` to disable array detection (all fields treated as scalars)

### Geo Type Conversion

**`geo_point`** values are converted to GeoJSON Point format:

| Input Format | Example                            | Output                                             |
| ------------ | ---------------------------------- | -------------------------------------------------- |
| Object       | `{"lat": 40.7128, "lon": -74.006}` | `{"type":"Point","coordinates":[-74.006,40.7128]}` |
| Array        | `[-74.006, 40.7128]`               | `{"type":"Point","coordinates":[-74.006,40.7128]}` |
| String       | `"40.7128,-74.006"`                | `{"type":"Point","coordinates":[-74.006,40.7128]}` |
| WKT          | `"POINT (-74.006 40.7128)"`        | `{"type":"Point","coordinates":[-74.006,40.7128]}` |

**`geo_shape`** values are converted to GeoJSON:

| Input Format | Supported Types                                                                           |
| ------------ | ----------------------------------------------------------------------------------------- |
| GeoJSON      | Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, GeometryCollection |
| WKT          | POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION |

---

## Output Schema

All search functions (`es_search`, `es_query`) return a table with:

1. **`_id`** (VARCHAR): The Elasticsearch document ID
2. **Mapped fields**: Columns for each field in the index mapping, with inferred types
3. **`_unmapped_`** (JSON): A JSON object containing any fields present in documents but not in the mapping

### Example Schema

For an index with this mapping:

```json
{
  "properties": {
    "name": { "type": "text" },
    "price": { "type": "float" },
    "tags": { "type": "keyword" },
    "location": { "type": "geo_point" }
  }
}
```

The output schema would be:

| Column       | Type            | Description                    |
| ------------ | --------------- | ------------------------------ |
| `_id`        | `VARCHAR`       | Document ID                    |
| `name`       | `VARCHAR`       | Text field                     |
| `price`      | `FLOAT`         | Float field                    |
| `tags`       | `LIST(VARCHAR)` | Keyword field (array detected) |
| `location`   | `VARCHAR`       | GeoJSON Point string           |
| `_unmapped_` | `JSON`          | Dynamic/unmapped fields        |

### Unmapped Fields

The `_unmapped_` column captures fields that exist in documents but aren't defined in the index mapping. This is useful when:

- The index has `dynamic: true` and documents contain ad-hoc fields
- Different documents have different structures
- You want to explore data before defining a strict schema

```sql
-- Find documents with unmapped fields
SELECT _id, _unmapped_ FROM es_query(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test'
) WHERE _unmapped_ IS NOT NULL AND _unmapped_ != '{}';
```

---

## Internals

This section describes the extension's architecture for developers who want to understand the implementation or contribute.

### Source Code Structure

```
src/
├── include/
│   ├── elasticsearch_extension.hpp  # Extension class declaration
│   ├── es_client.hpp               # HTTP client interface
│   ├── es_common.hpp               # Shared utilities and type mapping
│   ├── es_search.hpp               # es_search function declaration
│   ├── es_query.hpp                # es_query function declaration
│   ├── es_aggregate.hpp            # es_aggregate function declaration
│   └── es_filter_translator.hpp    # Filter pushdown translator
├── elasticsearch_extension.cpp      # Extension entry point, function registration
├── es_client.cpp                   # HTTP client implementation (httplib + OpenSSL)
├── es_common.cpp                   # Type mapping, JSON parsing, geo conversion
├── es_search.cpp                   # es_search table function
├── es_query.cpp                    # es_query table function with pushdown
├── es_filter_translator.cpp        # SQL filter to ES Query DSL translation
└── es_aggregate.cpp                # es_aggregate table function
```

### Key Components

#### `ElasticsearchClient` (es_client.cpp)

HTTP client for Elasticsearch communication:

- Uses `duckdb_httplib_openssl` (cpp-httplib bundled with DuckDB)
- Implements Scroll API for paginated retrieval
- Handles authentication, SSL/TLS, and retries
- Methods: `ScrollSearch`, `ScrollNext`, `ClearScroll`, `GetMapping`, `Aggregate`

#### `ElasticsearchFilterTranslator` (es_filter_translator.cpp)

Translates DuckDB's `TableFilterSet` to Elasticsearch Query DSL:

- Handles comparison operators (=, !=, <, >, <=, >=)
- Handles NULL checks (IS NULL, IS NOT NULL)
- Handles IN lists and LIKE patterns
- Combines multiple filters with `bool.must`
- Special handling for text fields (uses `.keyword` subfield)

#### Type Mapping (es_common.cpp)

- `BuildDuckDBTypeFromMapping()`: Converts ES field definition to DuckDB LogicalType
- `ParseMapping()`: Recursively processes ES mapping properties
- `MergeMappingsFromIndices()`: Handles multi-index queries with schema merging
- `DetectArrayFields()`: Samples documents to identify array fields

#### Geo Conversion (es_common.cpp)

- `GeoPointToGeoJSON()`: Handles all geo_point formats (object, array, string, WKT)
- `GeoShapeToGeoJSON()`: Converts geo_shape to GeoJSON
- `WktToGeoJSON()`: Full WKT parser supporting all geometry types

### Design Decisions

#### Direct httplib Usage

The extension uses cpp-httplib directly rather than DuckDB's `HTTPUtil` wrapper. This was chosen for:

- Simpler implementation without threading `ClientContext` through all calls
- Full control over request/response handling
- Self-contained extension without deep DuckDB integration

**Trade-off**: HTTP logging via `SET enable_http_logging=true` is not supported.

#### JSON Parsing with yyjson

Uses DuckDB's bundled yyjson library for JSON parsing:

- High performance for large documents
- Mutable document API for query building
- Immutable API for response parsing

#### Array Detection via Sampling

Since Elasticsearch mappings don't distinguish arrays from scalars:

- Sample N documents (default 100) during bind
- Check each field for array values
- Wrap detected array fields in `LIST(...)`
- Trade-off between accuracy and bind-time overhead

#### Filter Pushdown Strategy

Filters are translated conservatively:

- Only push filters that can be exactly represented in ES Query DSL
- Unsupported filters fall back to client-side evaluation
- Text fields use `.keyword` for exact matching (assumes default mapping)

### Extension Points

#### Adding New Filter Types

1. Add a new case in `ElasticsearchFilterTranslator::TranslateFilter()`
2. Implement the translation to ES Query DSL
3. Handle the filter type's specific value types

#### Supporting New ES Types

1. Add the type mapping in `BuildDuckDBTypeFromMapping()` (es_common.cpp)
2. Add value extraction logic in `SetValueFromJson()` (es_common.cpp)
3. Handle any special serialization needs

#### Adding New Table Functions

1. Create new header and source files (e.g., `es_newfunction.hpp/cpp`)
2. Define bind data struct, global state struct
3. Implement Bind, InitGlobal, and Scan functions
4. Register in `elasticsearch_extension.cpp`

---

## Sample Data Setup

The repository includes sample data for testing in `test/sample-data.jsonl`.

### Creating the Test Index

```bash
# Create the index with mapping
curl -X PUT "localhost:9200/test" \
  -H "Content-Type: application/json" \
  -u elastic:test \
  -d @test/index-config.json

# Load the sample data
curl -X POST "localhost:9200/_bulk" \
  -H "Content-Type: application/x-ndjson" \
  -u elastic:test \
  --data-binary @test/sample-data.jsonl
```

### Sample Data Schema

The test index contains 10 documents with the following fields:

| Field        | Type       | Description                                       |
| ------------ | ---------- | ------------------------------------------------- |
| `name`       | text       | Person's full name                                |
| `colors`     | keyword[]  | Favorite colors (array)                           |
| `amount`     | integer    | Integer amount                                    |
| `price`      | float      | Price value                                       |
| `deprecated` | boolean    | Deprecation flag (some docs)                      |
| `birth_date` | date       | Birth date                                        |
| `location`   | geo_point  | Location (various formats)                        |
| `geometry`   | geo_shape  | Geometry (Point, LineString, Polygon, MultiPoint) |
| `ip_address` | ip         | IP address                                        |
| `employee`   | object     | Nested object with name and salary                |
| `users`      | nested     | Array of user objects                             |
| `extra`      | (unmapped) | Dynamic field (various types)                     |

### Verifying the Setup

```sql
-- Count documents
SELECT count(*) FROM es_query(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test'
);
-- Expected: 10

-- Check schema
DESCRIBE SELECT * FROM es_query(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test'
);
```

---

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! This extension follows the conventions and guidelines used by the DuckDB project and its extension ecosystem.

### Getting Started

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests (`make test`)
5. Run the formatter (`make format-fix`)
6. Commit your changes
7. Push to your fork and submit a pull request

### Development Guidelines

#### Code Style

This project follows DuckDB's coding conventions:

- **Indentation**: Use tabs for indentation, spaces for alignment
- **Line length**: Lines should not exceed 120 columns
- **Formatting**: Run `make format-fix` before committing (uses `clang-format` version 11.0.1)
- **Naming conventions**:
  - Files: lowercase with underscores (e.g., `es_client.cpp`)
  - Classes/Types: CamelCase (e.g., `ElasticsearchClient`)
  - Variables: lowercase with underscores (e.g., `scroll_id`)
  - Functions: CamelCase (e.g., `GetMapping`)

#### C++ Guidelines

- Do not use `malloc`; prefer smart pointers (`unique_ptr` over `shared_ptr`)
- Use `const` whenever possible
- Do not import namespaces (avoid `using namespace std`)
- Use `[u]int(8|16|32|64)_t` instead of `int`, `long`, etc.
- Use `idx_t` for offsets/indices/counts
- Prefer references over pointers as function arguments
- Use C++11 range-based for loops: `for (const auto& item : items)`
- Always use braces for `if` statements and loops

#### Testing

- **Preferred**: Write tests using the [sqllogictest framework](https://duckdb.org/dev/testing) (`.test` files in `test/sql/`)
- **C++ tests**: Only when testing behavior that can't be expressed in SQL (e.g., concurrent connections)
- Run fast unit tests: `make test` or `make unit`
- Test with different types: numerics, strings, and complex nested types
- Test error cases, not just the happy path
- All tests must pass before submitting a PR

#### Pull Request Guidelines

- Keep PRs focused and reasonably sized - large PRs are harder to review
- Clearly describe the problem and solution in the PR description
- Reference any related issues
- Ensure all CI checks pass
- Avoid "Draft" PRs; use issues or discussions for work-in-progress ideas

#### Error Handling

- Use exceptions only for errors that terminate a query (e.g., connection failures, invalid queries)
- Use `D_ASSERT` for programmer errors (conditions that should never occur)
- Assertions should never be triggered by user input

### Building for Development

For development work, use debug builds which include additional assertions and debugging symbols:

```bash
make debug
```

The debug build outputs to `build/debug/` with the same structure as release builds.

### Running Tests

```bash
# Run fast unit tests
make test

# Run the DuckDB shell with the extension loaded
./build/release/duckdb

# Run specific test file
./build/release/test/unittest "test/sql/elasticsearch.test"
```

### Extension Architecture

If you're adding new functionality, here's where different components live:

| Component          | Location                       | Description                                    |
| ------------------ | ------------------------------ | ---------------------------------------------- |
| Table functions    | `src/es_*.cpp`                 | Each table function (search, query, aggregate) |
| HTTP client        | `src/es_client.cpp`            | Elasticsearch communication                    |
| Type mapping       | `src/es_common.cpp`            | ES to DuckDB type conversions                  |
| Filter translation | `src/es_filter_translator.cpp` | SQL to Query DSL                               |
| Headers            | `src/include/`                 | Public interfaces                              |

### Publishing to Community Extensions

DuckDB has a [Community Extensions repository](https://github.com/duckdb/community-extensions) for distributing third-party extensions. Extensions built with the extension template (like this one) can be submitted by opening a PR with a descriptor file. Once accepted, users can install with:

```sql
INSTALL elasticsearch FROM community;
LOAD elasticsearch;
```

### Resources

- [DuckDB Extension Template](https://github.com/duckdb/extension-template)
- [DuckDB Extension CI Tools](https://github.com/duckdb/extension-ci-tools)
- [DuckDB Testing Documentation](https://duckdb.org/dev/testing)
- [DuckDB Community Extensions](https://duckdb.org/community_extensions/development)
- [DuckDB Discord](https://discord.com/invite/tcvwpjfnZx) - `#extension-development` channel

For major changes, please open an issue first to discuss the proposed changes.
