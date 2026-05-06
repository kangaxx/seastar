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

#include <seastar/core/future.hh>

namespace seastar::xtrader {

future<> runtime_engine::start() {
    if (_started) {
        return make_ready_future<>();
    }
    return _ctp.start().then([this] {
        _started = true;
    });
}

future<> runtime_engine::stop() {
    if (!_started) {
        return make_ready_future<>();
    }
    return _ctp.stop().then([this] {
        _started = false;
    });
}

future<domain::order_status> runtime_engine::submit_order(const domain::order_request& request) {
    if (!_started) {
        return make_ready_future<domain::order_status>(domain::order_status::rejected);
    }

    return _ctp.send_order(request).then([this, direction = request.direction, offset = request.offset, volume = request.volume] (domain::order_status st) {
        if (st == domain::order_status::accepted || st == domain::order_status::filled) {
            _positions.apply_trade(domain::position_delta {
                .direction = direction,
                .offset = offset,
                .traded_volume = volume,
            });
        }
        return st;
    });
}

const domain::position_book& runtime_engine::positions() const noexcept {
    return _positions;
}

} // namespace seastar::xtrader
