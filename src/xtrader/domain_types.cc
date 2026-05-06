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
#include <seastar/xtrader/domain_types.hh>

#include <algorithm>

namespace seastar::xtrader::domain {

int position_book::consume(int& today, int& yesterday, int volume) noexcept {
    const int from_today = std::min(today, volume);
    today -= from_today;
    volume -= from_today;

    const int from_yesterday = std::min(yesterday, volume);
    yesterday -= from_yesterday;
    volume -= from_yesterday;

    return volume;
}

void position_book::apply_trade(const position_delta& delta) {
    if (delta.traded_volume <= 0) {
        return;
    }

    int unhandled = delta.traded_volume;

    if (delta.offset == offset_flag::open) {
        if (delta.direction == side::buy) {
            _view.long_today += delta.traded_volume;
        } else {
            _view.short_today += delta.traded_volume;
        }
        return;
    }

    if (delta.direction == side::buy) {
        if (delta.offset == offset_flag::close_today) {
            _view.short_today = std::max(0, _view.short_today - delta.traded_volume);
            return;
        }
        if (delta.offset == offset_flag::close_yesterday) {
            _view.short_yesterday = std::max(0, _view.short_yesterday - delta.traded_volume);
            return;
        }
        unhandled = consume(_view.short_today, _view.short_yesterday, delta.traded_volume);
    } else {
        if (delta.offset == offset_flag::close_today) {
            _view.long_today = std::max(0, _view.long_today - delta.traded_volume);
            return;
        }
        if (delta.offset == offset_flag::close_yesterday) {
            _view.long_yesterday = std::max(0, _view.long_yesterday - delta.traded_volume);
            return;
        }
        unhandled = consume(_view.long_today, _view.long_yesterday, delta.traded_volume);
    }

    if (unhandled > 0) {
        // Keep the skeleton deterministic: do not allow negative inventory in bootstrap stage.
        if (delta.direction == side::buy) {
            _view.short_yesterday = std::max(0, _view.short_yesterday - unhandled);
        } else {
            _view.long_yesterday = std::max(0, _view.long_yesterday - unhandled);
        }
    }
}

} // namespace seastar::xtrader::domain
