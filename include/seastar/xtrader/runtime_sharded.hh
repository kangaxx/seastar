/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/xtrader/runtime_engine.hh>

#include <optional>

namespace seastar::xtrader {

class runtime_sharded {
public:
    future<> start();
    future<> stop();

    [[nodiscard]] future<domain::order_status> submit_order(const domain::order_request& request);
    future<> apply_trade_report(const domain::trade_report& report);
    [[nodiscard]] future<std::optional<domain::position_view>> snapshot_positions();

private:
    sharded<runtime_engine> _engines;
};

} // namespace seastar::xtrader
