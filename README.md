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
  This includes spatial predicates from the DuckDB
  [spatial](https://duckdb.org/docs/stable/core_extensions/spatial/overview)
  extension.
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

For more information including the setup for integration tests, refer to
[test/README.md](test/README.md).

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

## Table functions

### `elasticsearch_query`

The `elasticsearch_query` table function allows querying Elasticsearch indices.

#### Parameters

The following table lists the parameters that the function supports:

| Parameter name         | Type    | Default value          | Description                                 |
| ---------------------- | ------- | ---------------------- | ------------------------------------------- |
| `host`                 | VARCHAR | `localhost` (required) | Elasticsearch hostname or IP address        |
| `port`                 | INTEGER | `9200`                 | Elasticsearch HTTP port                     |
| `index`                | VARCHAR | – (required)           | Index name or pattern (e.g. `logs-*`)       |
| `query`                | VARCHAR | –                      | Optional Elasticsearch query clause         |
| `username`             | VARCHAR | –                      | Username for HTTP basic authentication      |
| `password`             | VARCHAR | –                      | Password for HTTP basic authentication      |
| `use_ssl`              | BOOLEAN | `false`                | Use HTTPS instead of HTTP                   |
| `verify_ssl`           | BOOLEAN | `true`                 | Verify SSL certificates                     |
| `timeout`              | INTEGER | `30000`                | Request timeout in milliseconds             |
| `max_retries`          | INTEGER | `3`                    | Maximum retry attempts for transient errors |
| `retry_interval`       | INTEGER | `100`                  | Initial retry wait time in milliseconds     |
| `retry_backoff_factor` | DOUBLE  | `2.0`                  | Exponential backoff multiplier              |
| `sample_size`          | INTEGER | `100`                  | Documents to sample for array detection     |

The `query` parameter accepts an Elasticsearch query clause (e.g.
`{"match": {"name": "alice"}}`), not a full request body. If provided, the
query is merged with any filters pushed down from SQL `WHERE` clauses using
`bool.must`.

#### Output schema

The `elasticsearch_query` function returns a table with:

1. `_id` (`VARCHAR`) – the Elasticsearch document ID.
1. Mapped fields – columns for each field in the index mapping with inferred
   types.
1. `_unmapped_` (`JSON`) – a JSON object containing any fields present in
   documents but not in the mapping.

#### How it works

1. Bind phase – fetches index mapping from Elasticsearch, infers DuckDB schema
   and optionally samples documents to detect array fields.
1. Filter pushdown – DuckDB's optimizer pushes `WHERE` clauses to the extension
   which translates them to Elasticsearch Query DSL.
1. Projection pushdown – only requested columns are included in the `_source`
   filter.
1. Limit pushdown – `LIMIT` and `OFFSET` clauses are pushed via an optimizer
   extension.
1. Scan phase – executes the optimized query using scroll API, fetches
   documents in batches and converts JSON to DuckDB values.

#### Examples

Basic query that fetches all documents:

```sql
SELECT * FROM elasticsearch_query(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test'
);
```

Query with filter and projection pushdown:

```sql
SELECT name, amount FROM elasticsearch_query(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test'
)
WHERE deprecated = true;
```

The extension translates this to the following Elasticsearch query:

```json
{
  "query": {
    "term": { "deprecated": true }
  },
  "_source": ["name", "amount"]
}
```

Base query combined with SQL filters:

```sql
SELECT name, price FROM elasticsearch_query(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test',
    query := '{"exists": {"field": "employee"}}'
)
WHERE price BETWEEN 2000 AND 6000;
```

The base query and SQL filters are merged using `bool.must`:

```json
{
  "query": {
    "bool": {
      "must": [
        { "exists": { "field": "employee" } },
        {
          "bool": {
            "must": [
              { "range": { "price": { "gte": 2000 } } },
              { "range": { "price": { "lte": 6000 } } }
            ]
          }
        }
      ]
    }
  },
  "_source": ["price", "name"]
}
```

Query with geospatial filter pushdown (requires the DuckDB spatial extension):

```sql
SELECT name FROM elasticsearch_query(
    host := 'localhost',
    index := 'test',
    username := 'elastic',
    password := 'test'
)
WHERE ST_Intersects(ST_GeomFromGeoJSON(geometry), ST_Point(-122.4194, 37.7749));
```

The extension translates this to the following Elasticsearch query:

```json
{
  "query": {
    "geo_shape": {
      "geometry": {
        "shape": { "type": "Point", "coordinates": [-122.4194, 37.7749] },
        "relation": "intersects"
      }
    }
  },
  "_source": ["name"]
}
```

## Filter pushdown

The following SQL expressions are translated to Elasticsearch Query DSL:

| SQL expression                   | Elasticsearch query                                                   |
| -------------------------------- | --------------------------------------------------------------------- |
| `column = value`                 | `{"term": {"column": value}}`                                         |
| `column != value`                | `{"bool": {"must_not": {"term": {"column": value}}}}`                 |
| `column < value`                 | `{"range": {"column": {"lt": value}}}`                                |
| `column > value`                 | `{"range": {"column": {"gt": value}}}`                                |
| `column <= value`                | `{"range": {"column": {"lte": value}}}`                               |
| `column >= value`                | `{"range": {"column": {"gte": value}}}`                               |
| `column IN (a, b, c)`            | `{"terms": {"column": [a, b, c]}}`                                    |
| `column LIKE 'prefix%'`          | `{"prefix": {"column": "prefix"}}`                                    |
| `column LIKE '%suffix'`          | `{"wildcard": {"column": {"value": "*suffix"}}}`                      |
| `column LIKE '%pattern%'`        | `{"wildcard": {"column": {"value": "*pattern*"}}}`                    |
| `column ILIKE 'pattern'`         | Case-insensitive wildcard query                                       |
| `column IS NULL`                 | `{"bool": {"must_not": {"exists": {"field": "column"}}}}`             |
| `column IS NOT NULL`             | `{"exists": {"field": "column"}}`                                     |
| `ST_Within(column, shape)`       | `{"geo_shape": {"column": {"shape": ..., "relation": "within"}}}`     |
| `ST_Contains(column, shape)`     | `{"geo_shape": {"column": {"shape": ..., "relation": "contains"}}}`   |
| `ST_Intersects(column, shape)`   | `{"geo_shape": {"column": {"shape": ..., "relation": "intersects"}}}` |
| `ST_Disjoint(column, shape)`     | `{"geo_shape": {"column": {"shape": ..., "relation": "disjoint"}}}`   |
| `ST_DWithin(column, point, N)`   | `{"geo_distance": {"distance": "Nm", "column": [lon, lat]}}`          |
| `ST_Distance(column, point) < N` | `{"geo_distance": {"distance": "Nm", "column": [lon, lat]}}`          |

The following table summarizes the pushdown behavior:

| Field type              | `=`, `!=` | `<`, `>`, `<=`, `>=` | `IN`   | `LIKE`, `ILIKE` | `IS NULL`, `IS NOT NULL` |
| ----------------------- | --------- | -------------------- | ------ | --------------- | ------------------------ |
| numeric                 | PUSHED    | PUSHED               | PUSHED | N/A             | PUSHED                   |
| date                    | PUSHED    | PUSHED               | PUSHED | N/A             | PUSHED                   |
| boolean                 | PUSHED    | N/A                  | PUSHED | N/A             | PUSHED                   |
| keyword                 | PUSHED    | PUSHED               | PUSHED | PUSHED          | PUSHED                   |
| text with `.keyword`    | PUSHED    | PUSHED               | PUSHED | PUSHED          | PUSHED                   |
| text without `.keyword` | ERROR     | ERROR                | ERROR  | ERROR           | PUSHED                   |
| nested object fields    | PUSHED    | PUSHED               | PUSHED | PUSHED          | PUSHED                   |
| array element access    | FILTER    | FILTER               | FILTER | FILTER          | FILTER                   |
| geo fields              | N/A       | N/A                  | N/A    | N/A             | N/A                      |

PUSHED – filter is translated to Elasticsearch Query DSL.  
ERROR – throws an error.  
FILTER – filter cannot be pushed down; handled by DuckDB's `FILTER` operator
after the scan.  
N/A – not applicable for this field type; geo fields use spatial predicates
instead (see below).

### Text fields

Elasticsearch `text` fields are analyzed (tokenized) and don't support exact
match queries like `term`. For fields with a `.keyword` subfield, filters are
automatically redirected to the `.keyword` subfield for exact matching. For
fields without `.keyword` subfield, an error is thrown. One possible solution
is to add a `.keyword` subfield to the Elasticsearch mapping. Another
workaround is using the `query` parameter:

```sql
SELECT * FROM elasticsearch_query(
    host := 'localhost',
    index := 'test',
    query := '{"match": {"description": "wireless headphones"}}'
);
```

### Geo fields

`geo_point` and `geo_shape` fields use spatial function predicates instead of
standard SQL operators. Pushdown requires the DuckDB
[spatial](https://duckdb.org/docs/stable/core_extensions/spatial/overview)
extension to be installed and loaded:

```sql
INSTALL spatial;
LOAD spatial;
```

The Elasticsearch geo field must be wrapped in `ST_GeomFromGeoJSON()` and the
other argument must be a constant geometry expression (e.g. `ST_Point()`,
`ST_GeomFromGeoJSON()`, `ST_MakeEnvelope()`).

The following spatial predicates are pushed down:

| Predicate       | Elasticsearch query | `ST_MakeEnvelope` optimization | Symmetric |
| --------------- | ------------------- | ------------------------------ | --------- |
| `ST_Within`     | `geo_shape`         | `geo_bounding_box`             | No        |
| `ST_Contains`   | `geo_shape`         | `geo_bounding_box`             | No        |
| `ST_Intersects` | `geo_shape`         | –                              | Yes       |
| `ST_Disjoint`   | `geo_shape`         | –                              | Yes       |
| `ST_DWithin`    | `geo_distance`      | –                              | Yes       |
| `ST_Distance`   | `geo_distance`      | –                              | Yes       |

`ST_Within` and `ST_Contains` are asymmetric – the Elasticsearch relation
depends on which argument is the field and which is the constant shape. For
example, `ST_Within(column, shape)` means field is within shape (relation
`within`), while `ST_Within(shape, column)` means shape is within field
(relation `contains`). `ST_Intersects` and `ST_Disjoint` are symmetric and
produce the same relation regardless of argument order. When `ST_MakeEnvelope`
is used as the constant geometry, the query is optimized to a more efficient
`geo_bounding_box` query instead of `geo_shape`.

`ST_DWithin` and `ST_Distance` comparisons are translated to Elasticsearch
`geo_distance` queries. `ST_DWithin(column, point, distance)` is equivalent to
`ST_Distance(column, point) <= distance`. For `ST_Distance`, the operators `<`,
`<=`, `>` and `>=` are supported. `<` and `<=` produce a `geo_distance` query
matching points within the given distance, while `>` and `>=` are wrapped in
`bool.must_not` to match points farther than the given distance. The distance
is specified in meters. Both argument orders are supported (e.g.
`ST_Distance(column, point)` and `ST_Distance(point, column)`) as well as
reversed operand order (e.g. `10000 > ST_Distance(column, point)`).

## Projection pushdown and filter pruning

When executing a query, the extension optimizes data transfer by only
requesting the columns that are actually needed:

- Projection pushdown – only columns referenced in the `SELECT` clause (and
  other parts of the query) are included in the Elasticsearch `_source` filter.
  This reduces network bandwidth and parsing overhead by excluding unnecessary
  fields from the response.
- Filter pruning – columns that are only used in `WHERE` clauses for pushed
  filters are excluded from the `_source` request. Since these filters are
  evaluated server-side by Elasticsearch, the actual field values don't need to
  be transferred back to DuckDB.

Consider the following query:

```sql
SELECT title, amount FROM elasticsearch_query(...)
WHERE in_stock = true AND category = 'electronics';
```

If both filters are pushed to Elasticsearch, the `_source` filter will only
include `["title", "amount"]`. The `in_stock` and `category` columns are pruned
since their values are not needed after server-side filtering.

## Limit and offset pushdown

`LIMIT` and `OFFSET` clauses are pushed to Elasticsearch via an optimizer
extension. This means:

- Small result sets are fetched efficiently without scrolling through all
  documents.
- The optimizer removes the `LIMIT` node from the query plan when pushdown
  succeeds.
- For `LIMIT N OFFSET M`, the extension fetches `N` + `M` documents and skips
  the first `M`.

## Type mapping

The following table summarizes Elasticsearch to DuckDB type mapping:

| Elasticsearch type | DuckDB type         | Notes                                            |
| ------------------ | ------------------- | ------------------------------------------------ |
| `text`             | `VARCHAR`           | Analyzed text; use `.keyword` for exact matching |
| `keyword`          | `VARCHAR`           | Not analyzed, exact values                       |
| `long`             | `BIGINT`            | 64-bit signed integer                            |
| `integer`          | `INTEGER`           | 32-bit signed integer                            |
| `short`            | `SMALLINT`          | 16-bit signed integer                            |
| `byte`             | `TINYINT`           | 8-bit signed integer                             |
| `double`           | `DOUBLE`            | 64-bit floating point                            |
| `float`            | `FLOAT`             | 32-bit floating point                            |
| `half_float`       | `FLOAT`             | 16-bit floating point                            |
| `boolean`          | `BOOLEAN`           | True/false                                       |
| `date`             | `TIMESTAMP`         | Parsed from ISO8601 or epoch                     |
| `ip`               | `VARCHAR`           | IP addresses as strings                          |
| `geo_point`        | `VARCHAR`           | Converted to GeoJSON `Point` type                |
| `geo_shape`        | `VARCHAR`           | Converted to relevant GeoJSON geometry type      |
| `object`           | `STRUCT(...)`       | Nested properties become struct fields           |
| `nested`           | `LIST(STRUCT(...))` | Always treated as array of objects               |

### Array handling

Elasticsearch mappings don't distinguish between scalar fields and arrays. The
extension detects arrays by sampling documents:

- Sample `sample_size` documents.
- If any document has an array value for a field, wrap the type in `LIST(...)`.
- Set `sample_size := 0` to disable array detection (all fields will be treated
  as scalars).

### Geospatial types

`geo_point` values are converted to GeoJSON `Point` type:

| Input format | Example                            | Output                                             |
| ------------ | ---------------------------------- | -------------------------------------------------- |
| object       | `{"lat": 40.7128, "lon": -74.006}` | `{"type":"Point","coordinates":[-74.006,40.7128]}` |
| array        | `[-74.006, 40.7128]`               | `{"type":"Point","coordinates":[-74.006,40.7128]}` |
| string       | `"40.7128,-74.006"`                | `{"type":"Point","coordinates":[-74.006,40.7128]}` |
| WKT          | `"POINT (-74.006 40.7128)"`        | `{"type":"Point","coordinates":[-74.006,40.7128]}` |

`geo_shape` values are converted to relevant GeoJSON geometry types:

| Input format | Supported types                                                        |
| ------------ | ---------------------------------------------------------------------- |
| GeoJSON      | `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString` etc. |
| WKT          | `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING` etc. |

### Unmapped fields

The `_unmapped_` column (`JSON` type) captures fields that exist in documents
but aren't defined in the index mapping. This is useful when:

- The index has `dynamic` set to `true` and documents contain ad-hoc fields.
- Different documents have different structures.
- You want to explore data before defining a strict schema.

The following query shows how to extract values from the `_unmapped_` column:

```sql
SELECT _unmapped_->>'$.extra.note' FROM elasticsearch_query(...)
WHERE _unmapped_ IS NOT NULL;
```

## HTTP logging

The extension supports DuckDB's HTTP
[logging](https://duckdb.org/docs/stable/operations_manual/logging/overview)
feature. Enable it to debug the requests sent to Elasticsearch:

```sql
CALL enable_logging('HTTP', storage = 'stdout');

SELECT * FROM elasticsearch_query(...);
```

## License

See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! This extension tries to follow the conventions and
guidelines used by the DuckDB project and its extension ecosystem.

The general process is as follows:

1. Fork the repository.
1. Create a feature branch.
1. Make your changes, including tests and documentation updates.
1. [Build the extension](#build-with-make).
1. [Run tests](#run-the-tests).
1. Run the linter (`make tidy-check`) and formatter (`make format-fix`).
1. Commit the changes.
1. Push to your fork and submit a pull request.

When submitting a pull request:

- Keep PRs focused and reasonably sized; large PRs are harder to review.
- Clearly describe the problem and solution in the PR description.
- Reference any related issues.
- Ensure all CI checks pass.
- Avoid draft PRs; use issues or discussions for work-in-progress ideas.

For major changes, please open an issue first to discuss the proposed changes.
