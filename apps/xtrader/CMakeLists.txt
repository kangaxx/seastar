#
# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# ============================================================
# 获取 Git 信息
# ============================================================
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

# 获取构建时间
string(TIMESTAMP BUILD_TIMESTAMP "%Y-%m-%d %H:%M:%S" UTC)

# ============================================================
# 配置头文件
# ============================================================
# 生成版本头文件到 include 目录
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/version.hh.in
    ${CMAKE_SOURCE_DIR}/include/seastar/xtrader/version.hh
    @ONLY)

# ============================================================
# 构建应用
# ============================================================

seastar_add_app (xtrader_demo
  SOURCES
    xtrader_demo.cc
    ../../src/xtrader/domain_types.cc
    ../../src/xtrader/ctp_gateway.cc
    ../../src/xtrader/runtime_engine.cc
    ../../src/xtrader/runtime_sharded.cc
    ../../src/xtrader/historical_data_manager.cc
  INCLUDES
    ${CMAKE_CURRENT_BINARY_DIR})

seastar_add_app (xtrader_cli
  SOURCES
    xtrader_cli.cc
    ../../src/xtrader/data_importer.cc
    ../../src/xtrader/historical_data_manager.cc
    ../../src/xtrader/redis_sync_client.cc
  INCLUDES
    ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries (app_xtrader_cli PRIVATE hiredis)
