set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(TEST_BASE_PATH "./tmp")
add_definitions("-DBASE_PATH=\"${TEST_BASE_PATH}\"")

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/Udf/udf_sample.cpp DESTINATION ${CMAKE_BINARY_DIR}/Tests/Udf)

add_executable(CodeGeneratorTest CodeGeneratorTest.cpp)
add_executable(ArrowBasedExecuteTest ArrowBasedExecuteTest.cpp)
add_executable(StringDictionaryTest StringDictionaryTest.cpp)
add_executable(StringTransformTest StringTransformTest.cpp)
add_executable(StringFunctionsTest StringFunctionsTest.cpp)
add_executable(EncoderTest EncoderTest.cpp)
if(NOT MSVC)
  add_executable(JSONTest JSONTest.cpp)
endif()
add_executable(DataRecyclerTest DataRecyclerTest.cpp)
add_executable(ParallelSortTest ParallelSortTest.cpp)

if(ENABLE_CUDA)
  message(DEBUG "Tests CUDA_COMPILATION_ARCH: ${CUDA_COMPILATION_ARCH}")
  set(MAPD_DEFINITIONS -DHAVE_CUDA)
  if (SUPPRESS_NULL_LOGGER_DEPRECATION_WARNINGS)
    list(APPEND MAPD_DEFINITIONS -DSUPPRESS_NULL_LOGGER_DEPRECATION_WARNINGS)
  endif()
endif()

add_executable(ResultSetTest ResultSetTest.cpp ResultSetTestUtils.cpp)
add_executable(FromTableReorderingTest FromTableReorderingTest.cpp)
add_executable(ResultSetBaselineRadixSortTest ResultSetBaselineRadixSortTest.cpp ResultSetTestUtils.cpp)
add_executable(UtilTest UtilTest.cpp)
add_executable(BumpAllocatorTest BumpAllocatorTest.cpp)
add_executable(TableFunctionsTest TableFunctionsTest.cpp)
add_executable(ArrayTest ArrayTest.cpp)
add_executable(GroupByTest GroupByTest.cpp)
add_executable(TopKTest TopKTest.cpp)
add_executable(CorrelatedSubqueryTest CorrelatedSubqueryTest.cpp)
add_executable(DateTimeUtilsTest Shared/DateTimeUtilsTest.cpp)
add_executable(ThreadingTest Shared/ThreadingTest.cpp)
add_executable(ThreadingTestSTD Shared/ThreadingTestStd.cpp)
add_executable(JoinHashTableTest JoinHashTableTest.cpp)
add_executable(CachedHashTableTest CachedHashTableTest.cpp)
add_executable(ColumnarResultsTest ColumnarResultsTest.cpp ResultSetTestUtils.cpp)
add_executable(SQLHintTest SQLHintTest.cpp)
if(NOT MSVC)
  add_executable(QuantileCpuTest Quantile/QuantileCpuTest.cpp)
endif()
add_executable(NoCatalogRelAlgTest NoCatalogRelAlgTest.cpp TestRelAlgDagBuilder.cpp)
add_executable(NoCatalogSqlTest NoCatalogSqlTest.cpp)
add_executable(ArrowStorageTest ArrowStorageTest.cpp)
add_executable(ArrowStorageSqlTest ArrowStorageSqlTest.cpp)
add_executable(ResultSetArrowConversion ResultSetArrowConversion.cpp)

if(NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  add_executable(UdfTest UdfTest.cpp)
endif()
if(ENABLE_CUDA)
  add_executable(GpuSharedMemoryTest GpuSharedMemoryTest.cpp ResultSetTestUtils.cpp)
endif()

# Tests + Microbenchmarks
add_executable(StringDictionaryBenchmark StringDictionaryBenchmark.cpp)

set(EXECUTE_TEST_LIBS gtest fmt::fmt ArrowQueryRunner ArrowStorage ${MAPD_LIBRARIES} ${Arrow_LIBRARIES} ${CMAKE_DL_LIBS} ${CUDA_LIBRARIES} ${Boost_LIBRARIES} ${ZLIB_LIBRARIES})

if(ENABLE_L0)
  add_executable(L0MgrExecuteTest L0MgrExecuteTest.cpp)
  target_include_directories(L0MgrExecuteTest PRIVATE ${LevelZero_INCLUDE_DIRS})
  target_link_libraries(L0MgrExecuteTest ${LevelZero_LIBRARIES} ${EXECUTE_TEST_LIBS})
  target_link_libraries(L0MgrExecuteTest L0Mgr)

  find_package(LLVMSPIRVLib REQUIRED COMPONENTS ${LLVMSPIRVLib_COMPONENTS})
  target_include_directories(L0MgrExecuteTest PRIVATE ${LLVMSPIRVLib_INCLUDE_DIRS})
  target_link_libraries(L0MgrExecuteTest ${LLVMSPIRVLib_LIBRARIES} ${EXECUTE_TEST_LIBS})

  add_test(L0MgrExecuteTest L0MgrExecuteTest ${TEST_ARGS})

  add_executable(DataMgrWithL0Test DataMgrWithL0Test.cpp)
  target_link_libraries(DataMgrWithL0Test DataMgr ${EXECUTE_TEST_LIBS})
  add_test(DataMgrWithL0Test DataMgrWithL0Test ${TEST_ARGS})
endif()

target_link_libraries(ResultSetTest ${EXECUTE_TEST_LIBS})
target_link_libraries(ColumnarResultsTest ${EXECUTE_TEST_LIBS})
target_link_libraries(FromTableReorderingTest ${EXECUTE_TEST_LIBS})
target_link_libraries(ResultSetBaselineRadixSortTest ${EXECUTE_TEST_LIBS})
target_link_libraries(UtilTest Utils gtest Logger Shared ${Boost_LIBRARIES})
target_link_libraries(StringTransformTest Logger Shared gtest ${Boost_LIBRARIES})
target_link_libraries(StringFunctionsTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CodeGeneratorTest ${EXECUTE_TEST_LIBS})
target_link_libraries(ArrowBasedExecuteTest ${EXECUTE_TEST_LIBS})
target_link_libraries(TableFunctionsTest ${EXECUTE_TEST_LIBS})
target_link_libraries(ArrayTest ${EXECUTE_TEST_LIBS})
target_link_libraries(GroupByTest ${EXECUTE_TEST_LIBS})
target_link_libraries(BumpAllocatorTest ${EXECUTE_TEST_LIBS})
target_link_libraries(TopKTest ${EXECUTE_TEST_LIBS})

if(ENABLE_FOLLY) 
  target_link_libraries(StringDictionaryTest gtest StringDictionary Logger Utils $<$<AND:$<CXX_COMPILER_ID:GNU>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,9.0>>:stdc++fs> ${CMAKE_DL_LIBS} ${Folly_LIBRARIES} ${Boost_LIBRARIES} ${ZLIB_LIBRARIES})
else()
  target_link_libraries(StringDictionaryTest gtest StringDictionary Logger Utils $<$<AND:$<CXX_COMPILER_ID:GNU>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,9.0>>:stdc++fs> ${CMAKE_DL_LIBS} ${Boost_LIBRARIES} ${ZLIB_LIBRARIES})
endif()

target_link_libraries(CorrelatedSubqueryTest ${EXECUTE_TEST_LIBS})
target_link_libraries(DateTimeUtilsTest gtest Logger Shared ${LLVM_LINKER_FLAGS})
target_link_libraries(ThreadingTest gtest Logger Shared ${LLVM_LINKER_FLAGS} ${TBB_LIBRARIES})
target_link_libraries(ThreadingTestSTD gtest Logger Shared ${LLVM_LINKER_FLAGS})
target_link_libraries(JoinHashTableTest ${EXECUTE_TEST_LIBS})
target_link_libraries(CachedHashTableTest ${EXECUTE_TEST_LIBS})
target_link_libraries(UtilTest OSDependent)
target_link_libraries(EncoderTest gtest ${Arrow_LIBRARIES} DataMgr Logger)
target_link_libraries(SQLHintTest ${EXECUTE_TEST_LIBS})
target_link_libraries(QuantileCpuTest gtest ${MAPD_LIBRARIES})
target_link_libraries(DataRecyclerTest ${EXECUTE_TEST_LIBS})
target_link_libraries(JSONTest gtest Logger Shared)
target_link_libraries(NoCatalogRelAlgTest ${EXECUTE_TEST_LIBS})
target_link_libraries(NoCatalogSqlTest ${EXECUTE_TEST_LIBS})
target_link_libraries(ArrowStorageTest ${EXECUTE_TEST_LIBS})
target_link_libraries(ArrowStorageSqlTest ${EXECUTE_TEST_LIBS})
target_link_libraries(ParallelSortTest ${EXECUTE_TEST_LIBS})
target_link_libraries(ResultSetArrowConversion ${EXECUTE_TEST_LIBS})

if(NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  target_link_libraries(UdfTest gtest UdfCompiler ${EXECUTE_TEST_LIBS})
endif()

if(ENABLE_FOLLY)
  target_link_libraries(StringDictionaryBenchmark benchmark gtest StringDictionary Logger Utils $<$<AND:$<CXX_COMPILER_ID:GNU>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,9.0>>:stdc++fs> ${CMAKE_DL_LIBS} ${Folly_LIBRARIES} ${Boost_LIBRARIES} ${ZLIB_LIBRARIES})
else()
  target_link_libraries(StringDictionaryBenchmark benchmark gtest StringDictionary Logger Utils $<$<AND:$<CXX_COMPILER_ID:GNU>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,9.0>>:stdc++fs> ${CMAKE_DL_LIBS} ${Boost_LIBRARIES} ${ZLIB_LIBRARIES})
endif()

if(ENABLE_CUDA)
  target_link_libraries(GpuSharedMemoryTest ${EXECUTE_TEST_LIBS})
endif()

set(TEST_ARGS "--gtest_output=xml:../")
add_test(UtilTest UtilTest ${TEST_ARGS})
add_test(ArrowBasedExecuteTest ArrowBasedExecuteTest ${TEST_ARGS})
add_test(NAME ArrowBasedExecuteTestColumnarOutput COMMAND ArrowBasedExecuteTest ${TEST_ARGS} "--enable-columnar-output")
add_test(NAME ArrowBasedExecuteTestSizeAgnostic COMMAND ArrowBasedExecuteTest ${TEST_ARGS} "--use-groupby-buffer-desc")
add_test(CodeGeneratorTest CodeGeneratorTest ${TEST_ARGS})
add_test(ResultSetTest ResultSetTest ${TEST_ARGS})
add_test(ColumnarResultsTest ColumnarResultsTest ${TEST_ARGS})
add_test(FromTableReorderingTest FromTableReorderingTest ${TEST_ARGS})
add_test(JoinHashTableTest JoinHashTableTest ${TEST_ARGS})
add_test(CachedHashTableTest CachedHashTableTest ${TEST_ARGS})
add_test(ResultSetBaselineRadixSortTest ResultSetBaselineRadixSortTest ${TEST_ARGS})
add_test(StringDictionaryTest StringDictionaryTest ${TEST_ARGS})
add_test(NAME StringDictionaryHashTest COMMAND StringDictionaryTest ${TEST_ARGS} "--enable-string-dict-hash-cache")
add_test(StringTransformTest StringTransformTest ${TEST_ARGS})
add_test(StringFunctionsTest StringFunctionsTest ${TEST_ARGS})
add_test(BumpAllocatorTest BumpAllocatorTest ${TEST_ARGS})
add_test(TableFunctionsTest TableFunctionsTest ${TEST_ARGS})
add_test(ArrayTest ArrayTest ${TEST_ARGS})
add_test(GroupByTest GroupByTest ${TEST_ARGS})
add_test(TopKTest TopKTest ${TEST_ARGS})
add_test(CorrelatedSubqueryTest CorrelatedSubqueryTest ${TEST_ARGS})
add_test(DateTimeUtilsTest DateTimeUtilsTest ${TEST_ARGS})
add_test(ThreadingTest ThreadingTest ${TEST_ARGS})
add_test(ThreadingTestSTD ThreadingTestSTD ${TEST_ARGS})
add_test(JoinHashTableTest JoinHashTableTest ${TEST_ARGS})
add_test(EncoderTest EncoderTest ${TEST_ARGS})
add_test(SQLHintTest SQLHintTest ${TEST_ARGS})
add_test(DataRecyclerTest DataRecyclerTest ${TEST_ARGS})
add_test(JSONTest JSONTest ${TEST_ARGS})
add_test(NoCatalogRelAlgTest NoCatalogRelAlgTest ${TEST_ARGS})
add_test(NoCatalogSqlTest NoCatalogSqlTest ${TEST_ARGS})
add_test(ArrowStorageTest ArrowStorageTest ${TEST_ARGS})
add_test(ArrowStorageSqlTest ArrowStorageSqlTest ${TEST_ARGS})
add_test(ParallelSortTest ParallelSortTest ${TEST_ARGS})
add_test(ResultSetArrowConversion ResultSetArrowConversion ${TEST_ARGS})

if(ENABLE_CUDA)
  add_test(GpuSharedMemoryTest GpuSharedMemoryTest ${TEST_ARGS})
endif()

if(NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  add_test(UdfTest UdfTest ${TEST_ARGS})
endif()

find_package(fmt REQUIRED)

#parse s3 credentials
file(READ aws/s3client.conf S3CLIENT_CONF)
if("${S3CLIENT_CONF}" MATCHES "AWS_ACCESS_KEY_ID=([^\n]+)")
  set(AWS_ACCESS_KEY_ID "${CMAKE_MATCH_1}")
endif()
if("${S3CLIENT_CONF}" MATCHES "AWS_SECRET_ACCESS_KEY=([^\n]+)")
  set(AWS_SECRET_ACCESS_KEY "${CMAKE_MATCH_1}")
endif()
if("${S3CLIENT_CONF}" MATCHES "AWS_REGION=([^\n]+)")
  set(AWS_REGION "${CMAKE_MATCH_1}")
endif()

set(TEST_PROGRAMS 
  ArrowBasedExecuteTest
  CodeGeneratorTest
  ResultSetTest
  ColumnarResultsTest
  FromTableReorderingTest
  ResultSetBaselineRadixSortTest
  BumpAllocatorTest
  TableFunctionsTest
  ArrayTest
  GroupByTest
  TopKTest
  CorrelatedSubqueryTest
  DateTimeUtilsTest
  ThreadingTest
  ThreadingTestSTD
  JoinHashTableTest
  StringFunctionsTest
  StringDictionaryTest
  EncoderTest
  SQLHintTest
  DataRecyclerTest
  JSONTest
  NoCatalogRelAlgTest
  NoCatalogSqlTest
  ArrowStorageTest
  ArrowStorageSqlTest
  ParallelSortTest
  ResultSetArrowConversion
)

if(ENABLE_CUDA)
  list(APPEND TEST_PROGRAMS GpuSharedMemoryTest)
endif()

if(NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  list(APPEND TEST_PROGRAMS UdfTest)
endif()

set(SANITY_TESTS ${TEST_PROGRAMS})
list(APPEND SANITY_TESTS ArrowBasedExecuteTestColumnarOutput)
list(APPEND SANITY_TESTS ArrowBasedExecuteTestSizeAgnostic)
list(APPEND SANITY_TESTS StringDictionaryHashTest)

set_tests_properties(${SANITY_TESTS} PROPERTIES LABELS "sanity")

add_custom_target(sanity_tests
    COMMAND mkdir -p tmp
    COMMAND touch tmp/DictPayload
    COMMAND env AWS_REGION=${AWS_REGION}
                AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
                AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
                ${CMAKE_CTEST_COMMAND} --verbose --label-regex sanity
    DEPENDS ${TEST_PROGRAMS}
    USES_TERMINAL)

add_custom_target(all_tests
    COMMAND mkdir -p tmp
    COMMAND touch tmp/DictPayload
    COMMAND env AWS_REGION=${AWS_REGION}
                AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
                AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
                ${CMAKE_CTEST_COMMAND} --verbose
    DEPENDS ${TEST_PROGRAMS} UtilTest StringDictionaryTest StringTransformTest
    USES_TERMINAL)

add_custom_target(topk_tests
    COMMAND mkdir -p tmp
    COMMAND touch tmp/DictPayload
    COMMAND ${CMAKE_CTEST_COMMAND} --verbose --tests-regex "\"(TopKTest)\""
    DEPENDS TopKTest)
