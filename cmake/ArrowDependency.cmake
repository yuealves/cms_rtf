# Prefer a system Arrow installation; the existing PyArrow dependency also ships
# the C++ headers and shared library, including in the offline replay environment.
find_package(Arrow CONFIG QUIET)
if(TARGET Arrow::arrow_shared)
  set(CMS_RTF_ARROW_TARGET Arrow::arrow_shared)
else()
  execute_process(COMMAND "${Python3_EXECUTABLE}" -c
    "import pathlib, pyarrow; p=pathlib.Path(pyarrow.get_library_dirs()[0]); print(pyarrow.get_include()); print(next(iter(sorted(p.glob('libarrow.so.*'))), p/'libarrow.so'))"
    RESULT_VARIABLE arrow_result OUTPUT_VARIABLE arrow_paths
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(NOT arrow_result EQUAL 0)
    message(FATAL_ERROR "C++ aggregation requires Arrow C++ or PyArrow; select Python3_EXECUTABLE or disable CMS_RTF_BUILD_AGGREGATE")
  endif()
  string(REPLACE "\n" ";" arrow_paths "${arrow_paths}")
  list(GET arrow_paths 0 arrow_include)
  list(GET arrow_paths 1 arrow_library)
  if(NOT EXISTS "${arrow_library}")
    message(FATAL_ERROR "Arrow shared library missing: ${arrow_library}")
  endif()
  add_library(cms_rtf_arrow SHARED IMPORTED)
  set_target_properties(cms_rtf_arrow PROPERTIES
    IMPORTED_LOCATION "${arrow_library}"
    INTERFACE_INCLUDE_DIRECTORIES "${arrow_include}")
  set(CMS_RTF_ARROW_TARGET cms_rtf_arrow)
endif()
