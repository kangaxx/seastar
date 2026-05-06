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
#include <seastar/xtrader/ctp_adapter.hh>

#include <seastar/core/future.hh>

namespace seastar::xtrader {

future<> ctp_adapter::start() {
    _started = true;
    return make_ready_future<>();
}

future<> ctp_adapter::stop() {
    _started = false;
    return make_ready_future<>();
}

future<domain::order_status> ctp_adapter::send_order(const domain::order_request& request) {
    if (!_started || !request.is_valid()) {
        return make_ready_future<domain::order_status>(domain::order_status::rejected);
    }
    return make_ready_future<domain::order_status>(domain::order_status::accepted);
}

} // namespace seastar::xtrader
