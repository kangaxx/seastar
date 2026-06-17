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
#include <seastar/xtrader/runtime_sharded.hh>

namespace seastar::xtrader {

future<> runtime_sharded::start() {
    if (_engines.local_is_initialized()) {
        return make_ready_future<>();
    }

    return _engines.start().then([this] {
        return _engines.invoke_on_all([] (runtime_engine& engine) {
            return engine.start();
        });
    });
}

future<> runtime_sharded::stop() {
    if (!_engines.local_is_initialized()) {
        return make_ready_future<>();
    }

    return _engines.stop();
}

future<domain::order_status> runtime_sharded::submit_order(const domain::order_request& request) {
    if (!_engines.local_is_initialized()) {
        return make_ready_future<domain::order_status>(domain::order_status::rejected);
    }

    return _engines.invoke_on(0, [request] (runtime_engine& engine) {
        return engine.submit_order(request);
    });
}

future<> runtime_sharded::apply_trade_report(const domain::trade_report& report) {
    if (!_engines.local_is_initialized()) {
        return make_ready_future<>();
    }

    return _engines.invoke_on(0, [report] (runtime_engine& engine) {
        return engine.apply_trade_report(report);
    });
}

future<std::optional<domain::position_view>> runtime_sharded::snapshot_positions() {
    if (!_engines.local_is_initialized()) {
        return make_ready_future<std::optional<domain::position_view>>(std::nullopt);
    }

    return _engines.invoke_on(0, [] (runtime_engine& engine) {
        return make_ready_future<std::optional<domain::position_view>>(engine.positions().snapshot());
    });
}

} // namespace seastar::xtrader
