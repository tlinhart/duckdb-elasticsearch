# DuckDB Elasticsearch Extension Architecture

## Overview

This document explains the entry point for DuckDB to the Elasticsearch extension and the calling chain for query execution. The extension provides three table functions: `es_search`, `es_query`, and `es_aggregate`.

## Extension Entry Point

### 1. Extension Loading (`elasticsearch_extension.cpp`)

The entry point for DuckDB to load this extension is defined using the `DUCKDB_CPP_EXTENSION_ENTRY` macro:

```cpp
extern "C" {
    DUCKDB_CPP_EXTENSION_ENTRY(elasticsearch, loader) {
        duckdb::LoadInternal(loader);
    }
}
```

**What it does:**
- This macro creates the external C function that DuckDB calls when loading the extension
- The function name is derived from the extension name: `elasticsearch_init`
- DuckDB's extension loader discovers and calls this function at runtime

### 2. Function Registration (`LoadInternal`)

When the entry point is called, it executes `LoadInternal()`:

```cpp
static void LoadInternal(ExtensionLoader &loader) {
    // Register table functions.
    RegisterElasticsearchSearchFunction(loader);
    RegisterElasticsearchQueryFunction(loader);
    RegisterElasticsearchAggregateFunction(loader);
}
```

**What it does:**
- Registers three table functions with DuckDB's function registry
- Each registration function sets up the function name, parameters, and callbacks

## Table Function Architecture

DuckDB table functions follow a lifecycle with three main phases:

1. **Bind Phase** - Schema determination and validation
2. **Init Phase** - State initialization and first data fetch
3. **Scan Phase** - Data retrieval and processing (called multiple times)

## Calling Chain for `es_search`

### Query Example
```sql
SELECT * FROM es_search(host='localhost', index='my-index', query='{"query": {"match_all": {}}}');
```

### Execution Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. DuckDB Query Planner                                      │
│    - Parses SQL query                                        │
│    - Identifies es_search table function call                │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. ElasticsearchSearchBind (es_search.cpp:76)               │
│    Purpose: Determine output schema before execution        │
│                                                              │
│    Steps:                                                    │
│    a) Parse function parameters (host, port, index, query)  │
│    b) Validate required parameters                          │
│    c) Create ElasticsearchClient                            │
│    d) Call client.GetMapping(index)                         │
│       └──> ElasticsearchClient::GetMapping                  │
│            └──> PerformRequestWithRetry("GET", path)        │
│                 └──> PerformRequest (es_client.cpp:28)      │
│                      └──> httplib HTTP GET request          │
│    e) Parse mapping JSON response                           │
│    f) Call MergeMappingsFromIndices (es_common.cpp)         │
│       - Extracts field names and types                      │
│       - Handles nested fields                               │
│    g) Call DetectArrayFields (es_common.cpp)                │
│       - Samples documents to detect array fields            │
│    h) Build output schema: [_id, ...fields..., _unmapped_]  │
│    i) Return ElasticsearchSearchBindData                    │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. ElasticsearchSearchInitGlobal (es_search.cpp:197)        │
│    Purpose: Initialize state and fetch first batch of data  │
│                                                              │
│    Steps:                                                    │
│    a) Create ElasticsearchSearchGlobalState                 │
│    b) Create ElasticsearchClient with config                │
│    c) Determine batch size (default 1000, or limit)         │
│    d) Call client.ScrollSearch(index, query, scroll_time)   │
│       └──> ElasticsearchClient::ScrollSearch               │
│            └──> PerformRequestWithRetry("POST", path, body) │
│                 └──> PerformRequest                         │
│                      └──> httplib HTTP POST request         │
│    e) Parse response JSON using yyjson                      │
│    f) Extract scroll_id for pagination                      │
│    g) Extract hits array and store in state                 │
│    h) Return initialized state                              │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. ElasticsearchSearchScan (es_search.cpp:256)              │
│    Purpose: Retrieve and process data rows                  │
│    Note: Called multiple times until all data is retrieved  │
│                                                              │
│    Steps:                                                    │
│    a) Check if finished or limit reached                    │
│    b) Process current batch of hits:                        │
│       - Extract _id field                                   │
│       - For each mapped column:                             │
│         └──> Call GetValueByPath (es_common.cpp)            │
│              - Navigate nested JSON structure               │
│         └──> Call SetValueFromJson (es_common.cpp)          │
│              - Convert JSON to DuckDB types                 │
│       - Collect unmapped fields                             │
│         └──> Call CollectUnmappedFields (es_common.cpp)     │
│    c) If current batch exhausted and more data available:   │
│       - Call client.ScrollNext(scroll_id)                   │
│         └──> ElasticsearchClient::ScrollNext               │
│              └──> PerformRequestWithRetry("POST", path)     │
│                   └──> PerformRequest                       │
│                        └──> httplib HTTP POST request       │
│       - Parse new batch                                     │
│       - Continue processing                                 │
│    d) Fill output DataChunk with processed rows             │
│    e) Return (DuckDB calls again if more data needed)       │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. Cleanup                                                   │
│    - ElasticsearchSearchGlobalState destructor called       │
│    - Calls client.ClearScroll(scroll_id)                    │
│      └──> ElasticsearchClient::ClearScroll                  │
│           └──> PerformRequestWithRetry("DELETE", path)      │
│                └──> PerformRequest                          │
│                     └──> httplib HTTP DELETE request        │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

**ElasticsearchSearchBindData**: Stores configuration and schema information
- Connection config (host, port, credentials, SSL settings)
- Index name and query
- Output schema (column names, types, field paths)
- ES type information for special handling

**ElasticsearchSearchGlobalState**: Maintains scan state across calls
- ElasticsearchClient instance
- Scroll ID for pagination
- Current batch of hits
- Row counters and limits

## Calling Chain for `es_query`

The `es_query` function is similar to `es_search` but includes advanced features:

### Query Example
```sql
SELECT name, age FROM es_query(host='localhost', index='users') WHERE age > 30 LIMIT 100;
```

### Execution Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. ElasticsearchQueryBind (es_query.cpp:213)                │
│    Similar to es_search but:                                │
│    - Builds type maps for filter translation                │
│    - Identifies text fields (need .keyword suffix)          │
│    - Returns ALL columns from mapping                       │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. ElasticsearchQueryInitGlobal (es_query.cpp:330)          │
│    Advanced features:                                        │
│                                                              │
│    a) **Projection Pushdown**                               │
│       - Receives column_ids from DuckDB optimizer           │
│       - Only requests needed columns from Elasticsearch     │
│       - Builds _source projection array                     │
│                                                              │
│    b) **Filter Pushdown**                                   │
│       - Receives filters from DuckDB optimizer              │
│       - Calls ElasticsearchFilterTranslator::TranslateFilters│
│         └──> Converts DuckDB filters to ES query DSL       │
│              (es_filter_translator.cpp)                     │
│       - Handles: =, <, >, <=, >=, IS NULL, IS NOT NULL     │
│       - Supports AND/OR combinations                        │
│                                                              │
│    c) **Query Merging**                                     │
│       - Calls BuildFinalQuery (es_query.cpp:98)             │
│       - Merges user's base query with pushed filters        │
│       - Uses bool.must to combine                           │
│       - Adds _source projection                             │
│       - Adds size/limit if specified                        │
│                                                              │
│    d) Execute optimized query via ScrollSearch              │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. ElasticsearchQueryScan (es_query.cpp:419)                │
│    - Processes only projected columns                       │
│    - Returns filtered and projected data                    │
└─────────────────────────────────────────────────────────────┘
```

### Filter Translation Example

DuckDB filter: `WHERE age > 30 AND status = 'active'`

Gets translated to Elasticsearch DSL:
```json
{
  "bool": {
    "must": [
      {"range": {"age": {"gt": 30}}},
      {"term": {"status.keyword": "active"}}
    ]
  }
}
```

## Calling Chain for `es_aggregate`

### Query Example
```sql
SELECT * FROM es_aggregate(
    host='localhost', 
    index='sales',
    query='{"aggs": {"total_revenue": {"sum": {"field": "amount"}}}}'
);
```

### Execution Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. ElasticsearchAggregateBind (es_aggregate.cpp:265)        │
│    Purpose: Execute aggregation and infer schema            │
│                                                              │
│    Steps:                                                    │
│    a) Parse parameters                                      │
│    b) Create ElasticsearchClient                            │
│    c) Call client.Aggregate(index, query)                   │
│       └──> ElasticsearchClient::Aggregate                  │
│            └──> PerformRequestWithRetry("POST", path, body) │
│                 └──> PerformRequest                         │
│                      └──> httplib HTTP POST request         │
│    d) Parse response and extract "aggregations" object      │
│    e) Call InferTypeFromJson (es_aggregate.cpp:16)          │
│       - Recursively analyzes JSON structure                 │
│       - Infers DuckDB types (STRUCT, LIST, etc.)           │
│    f) Store result JSON and inferred type                   │
│    g) Return schema: single column "aggregations"           │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. ElasticsearchAggregateInitGlobal (es_aggregate.cpp:352)  │
│    - Creates empty global state                             │
│    - No actual work (query already executed in bind)        │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. ElasticsearchAggregateScan (es_aggregate.cpp:358)        │
│    Purpose: Return the aggregation result                   │
│                                                              │
│    Steps:                                                    │
│    a) Parse stored JSON result                              │
│    b) Call SetValueFromJson (es_aggregate.cpp:146)          │
│       - Converts JSON to inferred DuckDB type               │
│       - Handles STRUCT, LIST, scalar types recursively      │
│    c) Return single row with aggregation result             │
│    d) Mark as finished                                      │
└─────────────────────────────────────────────────────────────┘
```

**Key Difference**: The aggregation is executed during the Bind phase (not Init phase) because the result structure determines the output schema.

## HTTP Client Layer

All three table functions use the `ElasticsearchClient` class for HTTP communication:

### ElasticsearchClient Methods

1. **GetMapping**: Retrieves index mapping metadata
   - HTTP GET to `/{index}/_mapping`
   
2. **ScrollSearch**: Initiates scrolling search
   - HTTP POST to `/{index}/_search?scroll={time}`
   
3. **ScrollNext**: Continues scrolling
   - HTTP POST to `/_search/scroll`
   
4. **ClearScroll**: Clears scroll context
   - HTTP DELETE to `/_search/scroll`
   
5. **Aggregate**: Executes aggregation query
   - HTTP POST to `/{index}/_search`

### Request Flow with Retry Logic

```
PerformRequestWithRetry (es_client.cpp:100)
    ↓
    Loop (up to max_retries):
        ↓
        PerformRequest (es_client.cpp:28)
            ↓
            Create httplib::Client
            Configure timeouts, SSL, auth
            Execute HTTP request
            ↓
        If status in RETRYABLE_STATUS_CODES (429, 500, 502, 503, 504):
            Wait with exponential backoff
            Retry
        Else:
            Return response
```

## Data Type Mapping

The extension maps Elasticsearch types to DuckDB types in `es_common.cpp`:

| Elasticsearch Type | DuckDB Type |
|-------------------|-------------|
| keyword, text | VARCHAR |
| long, integer, short, byte | BIGINT |
| double, float, half_float | DOUBLE |
| boolean | BOOLEAN |
| date | TIMESTAMP |
| nested | STRUCT |
| object | STRUCT |
| geo_point | VARCHAR (JSON) |
| Arrays | LIST[element_type] |

## JSON Processing

The extension uses the `yyjson` library for high-performance JSON parsing:

1. **Parsing**: `yyjson_read()` parses JSON strings
2. **Navigation**: `yyjson_obj_get()`, `GetValueByPath()` traverse JSON
3. **Type Checking**: `yyjson_is_str()`, `yyjson_is_arr()`, etc.
4. **Value Extraction**: `yyjson_get_str()`, `yyjson_get_sint()`, etc.
5. **Serialization**: `yyjson_val_write()` converts back to JSON string

## Summary

The DuckDB Elasticsearch extension follows a clear three-phase architecture:

1. **Entry Point**: `DUCKDB_CPP_EXTENSION_ENTRY` macro → `LoadInternal()` → Register functions
2. **Bind Phase**: Determine schema by querying Elasticsearch mapping
3. **Init Phase**: Initialize state and fetch first batch of data
4. **Scan Phase**: Process data and fetch additional batches as needed

Each table function (`es_search`, `es_query`, `es_aggregate`) follows this pattern but with different optimizations:
- `es_search`: Simple search with full result set
- `es_query`: Advanced with projection and filter pushdown
- `es_aggregate`: Aggregation with type inference

The HTTP communication is handled by `ElasticsearchClient` with retry logic, and all JSON processing uses the high-performance `yyjson` library.
