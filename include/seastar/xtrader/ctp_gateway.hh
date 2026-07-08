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
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/xtrader/domain_types.hh>
#include <seastar/xtrader/spsc_ring_buffer.hh>

namespace seastar::xtrader {

// === Gateway Configuration ===

struct gateway_config {
    sstring broker_id;
    sstring investor_id;
    sstring password;
    sstring auth_code;
    sstring app_id;
    sstring user_product_info;
    sstring md_front_addr;
    sstring td_front_addr;
    int account_shard_id = 1;  // fixed shard for account updates
};

// === Gateway Status ===

enum class gateway_status {
    disconnected,
    connecting,
    authenticating,
    logging_in,
    ready,
    error,
};

// === CTP Gateway ===
//
// Responsibilities (Shard 0 only):
// - Hold MdApi and TraderApi instances
// - Push CTP callbacks into SPSC ring buffers (zero-allocation in callbacks)
// - Drain ring buffers and route events to target shards via submit_to()
// - Execute ReqOrderInsert / ReqOrderAction serially
// - Manage OrderRef generation
//
// Forbidden:
// - Do not directly modify any shard's position_book or trading_account
// - Do not hold cross-shard strategy pointers
//
class ctp_gateway {
public:
    explicit ctp_gateway(gateway_config config);

    // Lifecycle
    future<> start();
    future<> stop();

    // Trading
    [[nodiscard]] future<domain::order_status> submit_order(const domain::order_request& request);
    [[nodiscard]] future<domain::order_status> cancel_order(const sstring& order_sys_id);

    // Status
    [[nodiscard]] gateway_status status() const noexcept { return _status; }

    // Shard routing
    static constexpr unsigned gateway_shard_id = 0;
    static unsigned instrument_shard_id(const sstring& instrument_id, unsigned num_shards);

    // P1-2 FIX: Explicit SPI callback declarations (stubs for Phase 3)
    // These methods are called from CTP API threads and push data into ring buffers.
    // Implementation must be async-safe (no blocking, no direct shard access).
    void on_market_data(const domain::market_data& data);      // Pushes to md_ring
    void on_trade_report(const domain::trade_report& report);   // Pushes to trader_ring
    void on_order_return(const domain::order& order);           // Pushes to trader_ring
    void on_cancel_return(const domain::order& order);          // Pushes to trader_ring
    void on_error(int error_id, const sstring& error_msg);      // Logs error

    // P1-1 FIX: Drain methods called by timer<>, not recursive scheduling
    void drain_md_ring();
    void drain_trader_ring();

private:
    gateway_config _config;
    gateway_status _status = gateway_status::disconnected;

    md_ring_buffer _md_ring;
    trader_ring_buffer _trader_ring;

    // P1-1 FIX: Use timer for periodic drain instead of recursive scheduling
    timer<> _md_drain_timer;
    timer<> _trader_drain_timer;

    uint64_t _order_ref_seq = 0;

    // Metrics (for observability)
    uint64_t _dropped_md_events = 0;
    uint64_t _dropped_trader_events = 0;
};

// === Shard Configuration Constants ===

inline constexpr unsigned default_gateway_shard = 0;
inline constexpr unsigned default_account_shard = 1;

// === Drain Loop Constants ===

inline constexpr size_t max_drain_per_tick = 1024;
inline constexpr size_t max_drain_us = 500;  // 500us budget per reactor tick

} // namespace seastar::xtrader
