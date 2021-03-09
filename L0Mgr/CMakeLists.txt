add_library(L0Mgr L0Mgr.cpp)
target_include_directories(L0Mgr PRIVATE ${LevelZero_INCLUDE_DIRS})
target_link_libraries(L0Mgr ${LevelZero_LIBRARY})