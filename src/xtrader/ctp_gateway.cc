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

    // Start drain loops on this shard (Shard 0)
    start_md_drain_loop();
    start_trader_drain_loop();

    _status = gateway_status::ready;
    return make_ready_future<>();
}

future<> ctp_gateway::stop() {
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

void ctp_gateway::start_md_drain_loop() {
    // Register high-priority periodic task to drain md_ring_buffer
    // This runs on Shard 0 Reactor thread only.
    //
    // Constraints per STEP-03 7.5:
    // - Must set batch limits (MAX_DRAIN_PER_TICK / MAX_DRAIN_US)
    // - Record backlog metrics
    //
    // TODO(Phase 3): Start periodic drain
}

void ctp_gateway::start_trader_drain_loop() {
    // Register high-priority periodic task to drain trader_ring_buffer
    // This runs on Shard 0 Reactor thread only.
    //
    // Constraints per STEP-03 7.1:
    // - trader_ring_buffer must never drop events (dropped_trader_events == 0)
    // - If queue is full, trigger risk control escalation
    //
    // TODO(Phase 3): Start periodic drain
}

void ctp_gateway::drain_md_ring() {
    // Drains md_ring_buffer and routes market_data to target shard via submit_to()
    //
    // Route: md_ring -> instrument_shard(hash(InstrumentID))
    //
    // TODO(Phase 3): Implement drain with batch limits
    // md_slot slot;
    // int drained = 0;
    // while (drained < MAX_DRAIN_PER_TICK && _md_ring.pop(slot)) {
    //     auto target = instrument_shard_id(slot.data.instrument_id, smp::count);
    //     (void)submit_to(target, [slot] {
    //         // process_market_data(slot)
    //     });
    //     ++drained;
    // }
}

void ctp_gateway::drain_trader_ring() {
    // Drains trader_ring_buffer and routes events to target shard via submit_to()
    //
    // Route: trade_rtn -> instrument_shard(hash(InstrumentID))
    //        order_rtn  -> instrument_shard(hash(InstrumentID))
    //
    // TODO(Phase 3): Implement drain with trade event zero-drop constraint
    // trader_slot slot;
    // while (_trader_ring.pop(slot)) {
    //     auto target = instrument_shard_id(slot.trade_data.instrument_id, smp::count);
    //     (void)submit_to(target, [slot] {
    //         // process_trade_return(slot) or process_order_return(slot)
    //     });
    // }
}

} // namespace seastar::xtrader
