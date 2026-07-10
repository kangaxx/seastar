find_package(Git QUIET)
if(Git_FOUND)
    execute_process(
        COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_COMMIT_HASH
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)
    execute_process(
        COMMAND ${GIT_EXECUTABLE} rev-parse --abbrev-ref HEAD
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_BRANCH
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)
else()
    set(GIT_COMMIT_HASH "unknown")
    set(GIT_BRANCH "unknown")
endif()

string(TIMESTAMP BUILD_TIMESTAMP "%Y-%m-%d %H:%M:%S" UTC)

configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/version.hh.in
    ${CMAKE_SOURCE_DIR}/include/seastar/xtrader/version.hh
    @ONLY)

seastar_add_app(xtrader_demo SOURCES
    xtrader_demo.cc
    ../../src/xtrader/domain_types.cc
    ../../src/xtrader/ctp_gateway.cc
    ../../src/xtrader/runtime_engine.cc
    ../../src/xtrader/runtime_sharded.cc
    ../../src/xtrader/historical_data_manager.cc)

target_include_directories(app_xtrader_demo PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_SOURCE_DIR}/include)

seastar_add_app(xtrader_cli SOURCES
    xtrader_cli.cc
    ../../src/xtrader/data_importer.cc
    ../../src/xtrader/historical_data_manager.cc
    ../../src/xtrader/redis_sync_client.cc)

target_include_directories(app_xtrader_cli PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_SOURCE_DIR}/include)

target_link_libraries(app_xtrader_cli PRIVATE hiredis)
