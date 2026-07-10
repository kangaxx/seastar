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
#include <seastar/xtrader/ctp_gateway.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/reactor.hh>

namespace seastar::xtrader {

ctp_gateway::ctp_gateway(gateway_config config)
    : _config(std::move(config))
{}

unsigned ctp_gateway::instrument_shard_id(const sstring& instrument_id, unsigned num_shards) {
    // hash(InstrumentID) % (num_shards - 1) + 1
    // Shard 0 is reserved for gateway, so instrument shards are 1..N
    if (num_shards <= 1) {
        return default_account_shard;
    }
    unsigned hash = 0;
    for (char c : instrument_id) {
        hash = hash * 31 + static_cast<unsigned>(c);
    }
    return (hash % (num_shards - 1)) + 1;
}

future<> ctp_gateway::start() {
    _status = gateway_status::connecting;

    // TODO(Phase 3): Initialize MdApi and TraderApi
    // - MdApi: connect to _config.md_front_addr, call ReqUserLogin
    // - TraderApi: connect to _config.td_front_addr, call ReqAuthenticate -> ReqUserLogin

    // P1-1 FIX: Use timer for periodic drain instead of recursive submit_high_priority_task
    // This prevents CPU thrashing under high load.
    _md_drain_timer.set_callback([this] { drain_md_ring(); });
    _md_drain_timer.arm_periodic(std::chrono::microseconds(max_drain_us));

    _trader_drain_timer.set_callback([this] { drain_trader_ring(); });
    _trader_drain_timer.arm_periodic(std::chrono::microseconds(100));  // 100us for trader ring

    _status = gateway_status::ready;
    return make_ready_future<>();
}

future<> ctp_gateway::stop() {
    // P1-1 FIX: Cancel timers before shutdown
    _md_drain_timer.cancel();
    _trader_drain_timer.cancel();

    // TODO(Phase 3): Graceful shutdown of CTP connections
    // - Cancel pending orders
    // - Call ReqUserLogout
    // - Destroy MdApi and TraderApi
    _status = gateway_status::disconnected;
    return make_ready_future<>();
}

future<domain::order_status> ctp_gateway::submit_order(const domain::order_request& request) {
    if (_status != gateway_status::ready) {
        return make_ready_future<domain::order_status>(domain::order_status::rejected);
    }

    // TODO(Phase 3): Fill CThostFtdcInputOrderField and call ReqOrderInsert
    // - Generate order_ref: ++_order_ref_seq
    // - Map domain::order_type to CTP OrderPriceType/TimeCondition/VolumeCondition
    // - Record in pending_orders_ map (keyed by order_ref)
    // - Call ReqOrderInsert

    // Skeleton: return accepted for valid requests
    if (request.is_valid()) {
        return make_ready_future<domain::order_status>(domain::order_status::accepted);
    }
    return make_ready_future<domain::order_status>(domain::order_status::rejected);
}

future<domain::order_status> ctp_gateway::cancel_order(const sstring& order_sys_id) {
    if (_status != gateway_status::ready) {
        return make_ready_future<domain::order_status>(domain::order_status::rejected);
    }

    // TODO(Phase 3): Fill CThostFtdcInputOrderActionField and call ReqOrderAction
    // - Look up order_sys_id in pending_orders_
    // - Fill OrderSysID, FrontID, SessionID from stored order info
    // - Call ReqOrderAction

    return make_ready_future<domain::order_status>(domain::order_status::rejected);
}

// === P1-1 FIX: Removed recursive drain loops ===
// Drain loops are now implemented using timer<>, scheduled in start().
// The old recursive submit_high_priority_task approach has been removed.

// === P1-2 FIX: Explicit SPI callback implementations ===
// These stubs push data into ring buffers. Called from CTP API threads.
// Thread safety: push() is lock-free (SPSC), safe to call from any thread.

void ctp_gateway::on_market_data(const domain::market_data& data) {
    // Called from CTP MdApi thread - push to ring buffer (async-safe)
    md_slot slot;
    slot.capture_ns = seastar::steady_clock_type::now().time_since_epoch().count();
    slot.data = data;

    if (!_md_ring.push(slot)) {
        // Ring buffer full - record dropped event
        ++_dropped_md_events;
        std::cerr << "[WARN] ctp_gateway: md_ring full, dropped market data for "
                  << data.instrument_id << std::endl;
    }
}

void ctp_gateway::on_trade_report(const domain::trade_report& report) {
    // Called from CTP TraderApi thread - push to ring buffer (async-safe)
    trader_slot slot;
    slot.capture_ns = seastar::steady_clock_type::now().time_since_epoch().count();
    slot.type = domain::event_type::trade;
    slot.trade_data = report;
    // Clear order data for trade events
    slot.order_data = domain::order{};

    if (!_trader_ring.push(slot)) {
        // P1-1 constraint: trader_ring must NEVER drop events
        ++_dropped_trader_events;
        std::cerr << "[ERROR] ctp_gateway: trader_ring full, dropped trade event for "
                  << report.instrument_id << " (CRITICAL: violates zero-drop guarantee)" << std::endl;
        // TODO(Phase 3): Trigger risk control escalation here
    }
}

void ctp_gateway::on_order_return(const domain::order& order) {
    // Called from CTP TraderApi thread - push to ring buffer (async-safe)
    trader_slot slot;
    slot.capture_ns = seastar::steady_clock_type::now().time_since_epoch().count();
    slot.type = domain::event_type::order;
    slot.order_data = order;
    // Clear trade data for order events
    slot.trade_data = domain::trade_report{};

    if (!_trader_ring.push(slot)) {
        ++_dropped_trader_events;
        std::cerr << "[ERROR] ctp_gateway: trader_ring full, dropped order event for "
                  << order.order_sys_id << std::endl;
    }
}

void ctp_gateway::on_cancel_return(const domain::order& order) {
    // Called from CTP TraderApi thread - push to ring buffer (async-safe)
    trader_slot slot;
    slot.capture_ns = seastar::steady_clock_type::now().time_since_epoch().count();
    slot.type = domain::event_type::cancel;
    slot.order_data = order;
    slot.trade_data = domain::trade_report{};

    if (!_trader_ring.push(slot)) {
        ++_dropped_trader_events;
        std::cerr << "[ERROR] ctp_gateway: trader_ring full, dropped cancel event for "
                  << order.order_sys_id << std::endl;
    }
}

void ctp_gateway::on_error(int error_id, const sstring& error_msg) {
    // Called from CTP API threads - log error (not pushed to ring)
    std::cerr << "[ERROR] ctp_gateway: CTP error " << error_id << ": " << error_msg << std::endl;
}

void ctp_gateway::drain_md_ring() {
    // Drains md_ring_buffer and routes market_data to target shard via submit_to()
    //
    // Route: md_ring -> instrument_shard(hash(InstrumentID))
    //
    // Implementation with batch limits to prevent reactor blocking:
    md_slot slot;
    size_t drained = 0;
    const auto start_time = seastar::steady_clock_type::now();

    while (drained < max_drain_per_tick) {
        // Check time budget
        const auto elapsed = seastar::steady_clock_type::now() - start_time;
        if (elapsed.count() > static_cast<long>(max_drain_us * 1000)) {
            // Exceeded time budget, defer remaining to next tick
            break;
        }

        if (!_md_ring.pop(slot)) {
            // Queue empty
            break;
        }

        // Route market data to target shard
        const auto target_shard = instrument_shard_id(slot.data.instrument_id, smp::shard_count());

        // Submit to target shard for processing
        // Note: This is async, but we don't wait for completion
        (void)submit_to(target_shard, [slot = std::move(slot)] {
            // TODO(Phase 3): Process market data on target shard
            // - Update local market data cache
            // - Trigger strategy callbacks if subscribed
        });

        ++drained;
    }
}

void ctp_gateway::drain_trader_ring() {
    // Drains trader_ring_buffer and routes events to target shard via submit_to()
    //
    // Route: trade_rtn -> instrument_shard(hash(InstrumentID)) or account_shard
    //        order_rtn  -> instrument_shard(hash(InstrumentID))
    //
    // CRITICAL: Zero-drop guarantee for trader_ring
    trader_slot slot;

    while (_trader_ring.pop(slot)) {
        // Determine target shard based on event type
        unsigned target_shard;

        if (slot.type == domain::event_type::trade) {
            // Trade events go to account shard for balance update
            target_shard = static_cast<unsigned>(_config.account_shard_id);
        } else {
            // Order events go to instrument shard for order state update
            if (slot.type == domain::event_type::order) {
                target_shard = instrument_shard_id(slot.order_data.instrument_id, smp::shard_count());
            } else {
                target_shard = gateway_shard_id;
            }
        }

        // Submit to target shard
        // Note: This is async but we guarantee the slot is consumed from the ring
        (void)smp::submit_to(target_shard, [slot = std::move(slot)] {
            // TODO(Phase 3): Process trade/order event on target shard
            // - For trade: update position_book, apply account delta
            // - For order: update pending order state
        });
    }
}

} // namespace seastar::xtrader
