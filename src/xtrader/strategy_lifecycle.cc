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
#include <seastar/xtrader/strategy_lifecycle.hh>

#include <iostream>

namespace seastar::xtrader {

strategy_context strategy_lifecycle_manager::make_default_context(
    const sstring& strategy_id) {
    strategy_context ctx;
    ctx.strategy_id = strategy_id;

    ctx.submit_order = [strategy_id](const domain::order_request& req) {
        std::cerr << "[WARN] strategy_context[" << strategy_id
                  << "]: submit_order called but not wired to runtime"
                  << std::endl;
        return make_ready_future<domain::order_status>(
            domain::order_status::rejected);
    };

    ctx.cancel_order = [strategy_id](const sstring& sys_id) {
        std::cerr << "[WARN] strategy_context[" << strategy_id
                  << "]: cancel_order called but not wired to runtime"
                  << std::endl;
        return make_ready_future<domain::order_status>(
            domain::order_status::rejected);
    };

    ctx.query_position = [] {
        return domain::position_view{};
    };

    ctx.query_account = [] {
        return nullptr;
    };

    return ctx;
}

future<> strategy_lifecycle_manager::register_strategy(
    std::shared_ptr<strategy_base> strategy) {
    if (!strategy) {
        return make_exception_future<>(std::runtime_error("null strategy"));
    }

    const auto& id = strategy->strategy_id();
    if (_strategies.find(id) != _strategies.end()) {
        return make_exception_future<>(
            std::runtime_error("duplicate strategy_id: " + id));
    }

    _strategies[id] = std::move(strategy);
    return make_ready_future<>();
}

future<> strategy_lifecycle_manager::init_all() {
    if (_initialized) {
        return make_ready_future<>();
    }

    for (auto& [id, strategy] : _strategies) {
        if (strategy->state() == strategy_state::failed) {
            continue;
        }

        auto ctx = make_default_context(id);
        try {
            strategy->on_init(ctx);
            strategy->set_state(strategy_state::initialized);
            _contexts[id] = std::move(ctx);
            std::cout << "[INFO] strategy_lifecycle_manager: on_init OK, strategy="
                      << id << std::endl;
        } catch (const std::exception& e) {
            strategy->set_state(strategy_state::failed);
            std::cerr << "[ERROR] strategy_lifecycle_manager: on_init FAILED, strategy="
                      << id << ", error=" << e.what() << std::endl;
        } catch (...) {
            strategy->set_state(strategy_state::failed);
            std::cerr << "[ERROR] strategy_lifecycle_manager: on_init FAILED, strategy="
                      << id << ", unknown error" << std::endl;
        }
    }

    _initialized = true;
    return make_ready_future<>();
}

future<> strategy_lifecycle_manager::start_all() {
    for (auto& [id, strategy] : _strategies) {
        if (strategy->state() != strategy_state::initialized) {
            continue;
        }

        auto it_ctx = _contexts.find(id);
        if (it_ctx == _contexts.end()) {
            strategy->set_state(strategy_state::failed);
            std::cerr << "[ERROR] strategy_lifecycle_manager: no context for strategy="
                      << id << std::endl;
            continue;
        }

        try {
            strategy->on_warmup(it_ctx->second);
            strategy->set_state(strategy_state::running);
            std::cout << "[INFO] strategy_lifecycle_manager: started strategy="
                      << id << std::endl;
        } catch (const std::exception& e) {
            strategy->set_state(strategy_state::failed);
            std::cerr << "[ERROR] strategy_lifecycle_manager: on_warmup FAILED, strategy="
                      << id << ", error=" << e.what() << std::endl;
        } catch (...) {
            strategy->set_state(strategy_state::failed);
            std::cerr << "[ERROR] strategy_lifecycle_manager: on_warmup FAILED, strategy="
                      << id << ", unknown error" << std::endl;
        }
    }
    return make_ready_future<>();
}

future<> strategy_lifecycle_manager::stop_all() {
    const sstring reason = "shutdown";

    for (auto& [id, strategy] : _strategies) {
        if (strategy->state() != strategy_state::running) {
            continue;
        }
        strategy->set_state(strategy_state::stopping);

        auto it_ctx = _contexts.find(id);
        if (it_ctx == _contexts.end()) {
            strategy->set_state(strategy_state::stopped);
            continue;
        }

        try {
            strategy->on_stop(it_ctx->second, reason);
        } catch (const std::exception& e) {
            std::cerr << "[ERROR] strategy_lifecycle_manager: on_stop FAILED, strategy="
                      << id << ", error=" << e.what() << std::endl;
        } catch (...) {
            std::cerr << "[ERROR] strategy_lifecycle_manager: on_stop FAILED, strategy="
                      << id << ", unknown error" << std::endl;
        }

        strategy->set_state(strategy_state::stopped);
        std::cout << "[INFO] strategy_lifecycle_manager: stopped strategy="
                  << id << std::endl;
    }

    _contexts.clear();
    _initialized = false;
    return make_ready_future<>();
}

future<> strategy_lifecycle_manager::stop_strategy(const sstring& strategy_id) {
    auto it = _strategies.find(strategy_id);
    if (it == _strategies.end()) {
        return make_exception_future<>(
            std::runtime_error("strategy not found: " + strategy_id));
    }

    auto& strategy = it->second;
    strategy->set_state(strategy_state::stopping);

    auto it_ctx = _contexts.find(strategy_id);
    if (it_ctx != _contexts.end()) {
        try {
            strategy->on_stop(it_ctx->second, "manual_stop");
        } catch (...) {
        }
    }

    strategy->set_state(strategy_state::stopped);
    _contexts.erase(strategy_id);
    std::cout << "[INFO] strategy_lifecycle_manager: stopped strategy="
              << strategy_id << std::endl;
    return make_ready_future<>();
}

std::shared_ptr<strategy_base> strategy_lifecycle_manager::get_strategy(
    const sstring& strategy_id) {
    auto it = _strategies.find(strategy_id);
    if (it != _strategies.end()) {
        return it->second;
    }
    return nullptr;
}

strategy_context* strategy_lifecycle_manager::get_strategy_context(
    const sstring& strategy_id) {
    auto it = _contexts.find(strategy_id);
    if (it != _contexts.end()) {
        return &it->second;
    }
    return nullptr;
}

} // namespace seastar::xtrader
