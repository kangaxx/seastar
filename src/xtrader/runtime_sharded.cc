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

#include <stdexcept>

namespace seastar::xtrader {

runtime_sharded::runtime_sharded(unsigned account_shard) noexcept
    : _account_shard_id(account_shard)
{}

future<> runtime_sharded::start() {
    if (_engines.local_is_initialized()) {
        return make_ready_future<>();
    }

    // P0 FIX: Unified initialization with failure checking
    // 1. Set account_shard_id and gateway_shard_id on all engines
    // 2. Initialize account_service on account shard (MUST succeed)
    // 3. Start each engine

    unsigned init_failures = 0;

    return _engines.start().then([this, &init_failures] {
        return _engines.invoke_on_all(
            [account_shard = _account_shard_id, gateway_shard = _gateway_shard, &init_failures] (runtime_engine& engine) {
            engine.set_account_shard(account_shard);
            engine.set_gateway_shard(gateway_shard);

            // P0 FIX: Initialize account service on account shard
            // If this is the account shard, initialization MUST succeed
            if (smp::shard_id() == account_shard) {
                if (!engine.init_account_service(1000000.0)) {  // Default pre-balance
                    std::cerr << "[FATAL] runtime_sharded::start: "
                              << "account service initialization failed on shard " << smp::shard_id()
                              << ". Cannot start - will cause position/account mismatch!" << std::endl;
                    ++init_failures;
                }
            }

            return engine.start();
        }).then([&init_failures] {
            if (init_failures > 0) {
                std::cerr << "[FATAL] runtime_sharded::start: "
                          << init_failures << " shard(s) failed to initialize. Aborting start!" << std::endl;
                return make_exception_future<>(std::runtime_error("Account service initialization failed"));
            }
            return make_ready_future<>();
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

    // FIX: Use _gateway_shard instead of hardcoded 0
    return _engines.invoke_on(_gateway_shard, [request] (runtime_engine& engine) {
        return engine.submit_order(request);
    });
}

future<> runtime_sharded::apply_trade_report(const domain::trade_report& report) {
    if (!_engines.local_is_initialized()) {
        return make_ready_future<>();
    }

    // FIX: Use _gateway_shard instead of hardcoded 0
    return _engines.invoke_on(_gateway_shard, [report] (runtime_engine& engine) {
        return engine.apply_trade_report(report);
    });
}

future<std::optional<domain::position_view>> runtime_sharded::snapshot_positions() {
    if (!_engines.local_is_initialized()) {
        return make_ready_future<std::optional<domain::position_view>>(std::nullopt);
    }

    // FIX: Use _gateway_shard instead of hardcoded 0
    return _engines.invoke_on(_gateway_shard, [] (runtime_engine& engine) {
        return make_ready_future<std::optional<domain::position_view>>(engine.positions().snapshot());
    });
}

} // namespace seastar::xtrader
