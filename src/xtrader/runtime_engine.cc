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
#include <seastar/xtrader/runtime_engine.hh>
#include <seastar/xtrader/trading_account_service.hh>
#include <seastar/core/smp.hh>

namespace seastar::xtrader {

runtime_engine::runtime_engine(unsigned account_shard)
    : _account_shard(account_shard)
{}

runtime_engine::~runtime_engine() = default;

future<> runtime_engine::start() {
    if (_started) {
        return make_ready_future<>();
    }

    // P1 FIX: Set shard identity at runtime, not construction time
    // This ensures consistency with actual smp::shard_id()
    _is_gateway_shard = (smp::shard_id() == _gateway_shard);

    _started = true;
    return make_ready_future<>();
}

future<> runtime_engine::stop() {
    if (!_started) {
        return make_ready_future<>();
    }
    _started = false;
    return make_ready_future<>();
}

bool runtime_engine::init_account_service(double pre_balance) {
    // Only initialize on account shard
    if (smp::shard_id() != _account_shard) {
        // Not an error - this shard is not the account shard
        return true;
    }

    if (_account_service) {
        std::cerr << "[ERROR] runtime_engine::init_account_service: "
                  << "account service already initialized on account shard " << smp::shard_id()
                  << ". This indicates a programming error!" << std::endl;
        return false;  // FIX: Return false to block startup
    }

    _account_service = std::make_unique<trading_account_service>();
    _account_service->initialize(pre_balance);

    std::cout << "[INFO] runtime_engine::init_account_service: "
              << "initialized on account shard " << smp::shard_id()
              << " with pre_balance=" << pre_balance << std::endl;
    return true;
}

future<domain::order_status> runtime_engine::submit_order(const domain::order_request& request) {
    if (!_started) {
        return make_ready_future<domain::order_status>(domain::order_status::rejected);
    }

    // P1 FIX: Use runtime shard identity for routing
    if (smp::shard_id() == _gateway_shard) {
        // On gateway shard: forward to ctp_gateway
        // TODO(Phase 3): Call ctp_gateway.submit_order()
        return make_ready_future<domain::order_status>(domain::order_status::accepted);
    } else {
        // Non-gateway shard: submit to gateway shard
        return submit_to(_gateway_shard, [request] {
            // TODO(Phase 3): Call ctp_gateway.submit_order() on gateway shard
            return domain::order_status::accepted;
        });
    }
}

future<> runtime_engine::apply_trade_report(const domain::trade_report& report) {
    if (!_started || report.volume <= 0) {
        return make_ready_future<>();
    }

    // Update position on current shard
    _positions.apply_trade(domain::position_delta {
        .direction = report.direction,
        .offset = report.offset,
        .traded_volume = report.volume,
    });

    // Build account delta for account shard
    domain::account_delta delta;
    delta.instrument_id = report.instrument_id;
    delta.direction = report.direction;
    delta.offset = report.offset;
    delta.traded_volume = report.volume;
    delta.price = report.price;
    delta.commission = report.commission;
    // Note: margin_change and close_profit_change will be calculated in Phase 3
    // based on position_book state and instrument_spec

    // For Phase 2: assume current shard is account shard
    // Phase 3 will implement proper cross-shard routing via runtime_sharded
    return submit_delta_local(delta);
}

future<bool> runtime_engine::submit_delta_local(const domain::account_delta& delta) {
    // FIX: Only operate on local shard - no cross-shard this capture
    if (!_account_service) {
        std::cerr << "[ERROR] runtime_engine::submit_delta_local: "
                  << "_account_service is nullptr on shard " << smp::shard_id()
                  << ", dropping account delta for instrument " << delta.instrument_id
                  << ". This may cause position/account mismatch!" << std::endl;
        return make_ready_future<bool>(false);
    }
    bool success = _account_service->apply_delta(delta);
    return make_ready_future<bool>(success);
}

void runtime_engine::set_account_service(std::unique_ptr<trading_account_service> service) {
    _account_service = std::move(service);
}

const domain::position_book& runtime_engine::positions() const noexcept {
    return _positions;
}

} // namespace seastar::xtrader
