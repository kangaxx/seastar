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
#include <seastar/xtrader/domain_types.hh>

#include <memory>

namespace seastar::xtrader {

// Forward declaration for gateway
class ctp_gateway;
class trading_account_service;

// runtime_engine manages per-shard trading state.
// Each shard holds:
//   - position_book: local instrument positions
//   - trading_account_service: (on account shard) account balance and margin
//   - gateway: (on gateway shard) CTP connection and order routing
class runtime_engine {
public:
    runtime_engine() = default;
    ~runtime_engine() = default;

    runtime_engine(runtime_engine&&) noexcept = default;
    runtime_engine& operator=(runtime_engine&&) noexcept = default;

    // Disable copy (contains unique_ptr)
    runtime_engine(const runtime_engine&) = delete;
    runtime_engine& operator=(const runtime_engine&) = delete;

    explicit runtime_engine(unsigned account_shard);

    future<> start();
    future<> stop();

    [[nodiscard]] future<domain::order_status> submit_order(const domain::order_request& request);
    future<> apply_trade_report(const domain::trade_report& report);

    // Submit account delta to local account service (ONLY for local shard)
    // Remote shard routing should be done by runtime_sharded
    future<bool> submit_delta_local(const domain::account_delta& delta);

    [[nodiscard]] const domain::position_book& positions() const noexcept;

    // Access account service on this shard (if this shard is account_shard)
    trading_account_service* local_account_service() noexcept { return _account_service.get(); }
    void set_account_service(std::unique_ptr<trading_account_service> service);

    // Check if this engine is the gateway shard
    bool is_gateway_shard() const noexcept { return _is_gateway_shard; }

    // P0 FIX: Set account shard ID (called during startup to propagate config)
    void set_account_shard(unsigned shard_id) noexcept { _account_shard = shard_id; }
    unsigned account_shard() const noexcept { return _account_shard; }

    // P0 FIX: Set gateway shard ID
    void set_gateway_shard(unsigned shard_id) noexcept { _gateway_shard = shard_id; }
    unsigned gateway_shard() const noexcept { return _gateway_shard; }

    // P0 FIX: Initialize account service on account shard
    // Returns: true if initialized, false if not account shard
    bool init_account_service(double pre_balance = 1000000.0);

    // P0 FIX: Check initialization status
    bool is_account_service_ready() const noexcept {
        return _account_shard == smp::shard_id() && _account_service != nullptr;
    }

private:
    domain::position_book _positions;
    std::unique_ptr<trading_account_service> _account_service;
    unsigned _account_shard = 1;
    unsigned _gateway_shard = 0;  // P1 FIX: explicit gateway shard tracking
    bool _is_gateway_shard = false;
    bool _started = false;
};

} // namespace seastar::xtrader
