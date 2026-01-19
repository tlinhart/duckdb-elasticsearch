# Testing the Extension

This directory contains tests for the DuckDB Elasticsearch extension using
[sqllogictest](https://duckdb.org/dev/sqllogictest/intro.html).

Unit tests run without any external dependencies. Integration tests require a
running Elasticsearch (version 8.x or newer) instance with security enabled.

Start a single-node Elasticsearch cluster in Docker:

```shell
docker run -d --name elastic-test \
  -p 9200:9200 \
  -m 1g \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=true" \
  -e "xpack.ml.enabled=false" \
  -e "ELASTIC_PASSWORD=test" \
  -e "ES_JAVA_OPTS=-Xms512m -Xmx512m" \
  docker.elastic.co/elasticsearch/elasticsearch:8.17.0
```

Wait for the server to become available and load sample data:

```shell
./test/setup-elasticsearch.sh
```

Run tests with integration tests enabled:

```shell
export ELASTICSEARCH_TEST_SERVER_AVAILABLE=1
make test
```

Clean up:

```shell
docker rm -f elastic-test
```
