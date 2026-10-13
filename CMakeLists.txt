cmake_minimum_required(VERSION 3.20)
project(cms_rtf CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
add_compile_options(-O3 -Wall -Wextra)

add_library(cms_rtf_core src/book.cpp)
target_include_directories(cms_rtf_core PUBLIC include)

add_executable(cms_index500ms_day app/main_day.cpp)
target_link_libraries(cms_index500ms_day PRIVATE cms_rtf_core)

add_executable(cms_index500ms_mt app/cms_index500ms_mt.cpp)
target_link_libraries(cms_index500ms_mt PRIVATE cms_rtf_core)

option(CMS_RTF_BUILD_AGGREGATE "Build native aggregation and Arrow output" ON)
if(CMS_RTF_BUILD_AGGREGATE)
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  include(cmake/ArrowDependency.cmake)
  set(catalog_header "${CMAKE_CURRENT_BINARY_DIR}/generated/cms_rtf/catalog.h")
  add_custom_command(OUTPUT "${catalog_header}"
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/scripts/generate_catalog.py"
      "${CMAKE_CURRENT_SOURCE_DIR}/python/cms_rtf/catalog.py" "${catalog_header}"
    DEPENDS scripts/generate_catalog.py python/cms_rtf/catalog.py VERBATIM)
  add_executable(cms_index500ms_aggregate app/aggregate.cpp "${catalog_header}")
  target_include_directories(cms_index500ms_aggregate PRIVATE include "${CMAKE_CURRENT_BINARY_DIR}/generated")
  target_link_libraries(cms_index500ms_aggregate PRIVATE ${CMS_RTF_ARROW_TARGET})
  # Match the separate multiply/add operations of the existing NumPy contract.
  target_compile_options(cms_index500ms_aggregate PRIVATE -ffp-contract=off)
endif()

enable_testing()
add_executable(test_book tests/cpp/test_book.cpp)
target_link_libraries(test_book PRIVATE cms_rtf_core)
add_test(NAME book COMMAND test_book)

add_executable(test_flow tests/cpp/test_flow.cpp)
target_link_libraries(test_flow PRIVATE cms_rtf_core)
add_test(NAME flow COMMAND test_flow)

add_executable(test_runner tests/cpp/test_runner.cpp)
target_link_libraries(test_runner PRIVATE cms_rtf_core)
add_test(NAME runner COMMAND test_runner)

add_executable(test_ring tests/cpp/test_ring.cpp)
target_link_libraries(test_ring PRIVATE cms_rtf_core)
add_test(NAME ring COMMAND test_ring)

add_executable(test_identity_table tests/cpp/test_identity_table.cpp)
target_include_directories(test_identity_table PRIVATE include)
add_test(NAME identity_table COMMAND test_identity_table)

# Release builds must still execute the assertions in the original unit tests.
target_compile_options(test_book PRIVATE -UNDEBUG)
target_compile_options(test_flow PRIVATE -UNDEBUG)
