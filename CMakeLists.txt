cmake_minimum_required(VERSION 3.5)

# Set extension name here.
set(TARGET_NAME elasticsearch)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be
# added in vcpkg.json and then used in cmake with find_package.
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(duckdb/third_party/httplib)

set(EXTENSION_SOURCES src/elasticsearch_extension.cpp
                      src/es_client.cpp
                      src/es_common.cpp
                      src/es_search.cpp
                      src/es_query.cpp
                      src/es_filter_translator.cpp
                      src/es_aggregate.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library and the loadable extension.
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
