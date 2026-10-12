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

enable_testing()
add_executable(test_book tests/cpp/test_book.cpp)
target_link_libraries(test_book PRIVATE cms_rtf_core)
add_test(NAME book COMMAND test_book)

add_executable(test_flow tests/cpp/test_flow.cpp)
target_link_libraries(test_flow PRIVATE cms_rtf_core)
add_test(NAME flow COMMAND test_flow)
