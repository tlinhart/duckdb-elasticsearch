cmake_minimum_required(VERSION 3.5)

# Set extension name here.
set(TARGET_NAME elasticsearch)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be
# added in vcpkg.json and then used in cmake with find_package.
find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/elasticsearch_extension.cpp
                      src/elasticsearch_cache.cpp
                      src/elasticsearch_client.cpp
                      src/elasticsearch_common.cpp
                      src/elasticsearch_query.cpp
                      src/elasticsearch_filter_pushdown.cpp
                      src/elasticsearch_optimizer.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL and libcurl in both the static library and the loadable extension.
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
