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

#include <seastar/core/sstring.hh>

namespace seastar::xtrader::domain {

enum class side {
    buy,
    sell,
};

enum class offset_flag {
    open,
    close,
    close_today,
    close_yesterday,
};

enum class hedge_flag {
    speculation,
    hedge,
    arbitrage,
};

enum class order_status {
    submitted,
    accepted,
    partially_filled,
    filled,
    rejected,
};

struct instrument_spec {
    sstring instrument_id;
    int volume_multiple = 1;
    double price_tick = 0.0;
};

struct order_request {
    sstring strategy_id;
    sstring instrument_id;
    side direction = side::buy;
    offset_flag offset = offset_flag::open;
    hedge_flag hedge = hedge_flag::speculation;
    int volume = 0;
    double price = 0.0;

    [[nodiscard]] bool is_valid() const noexcept {
        return !strategy_id.empty() && !instrument_id.empty() && volume > 0 && price > 0.0;
    }
};

struct position_view {
    int long_today = 0;
    int long_yesterday = 0;
    int short_today = 0;
    int short_yesterday = 0;
};

struct position_delta {
    side direction = side::buy;
    offset_flag offset = offset_flag::open;
    int traded_volume = 0;
};

class position_book {
public:
    void apply_trade(const position_delta& delta);

    [[nodiscard]] const position_view& snapshot() const noexcept {
        return _view;
    }

private:
    static int consume(int& today, int& yesterday, int volume) noexcept;

private:
    position_view _view;
};

} // namespace seastar::xtrader::domain
