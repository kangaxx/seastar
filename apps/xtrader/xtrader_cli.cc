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
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/xtrader/historical_data_manager.hh>
#include <seastar/xtrader/redis_sync_client.hh>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace {

constexpr const char* kLatestKey = "xtrader:hist:dataset:latest:v1";
constexpr const char* kQueueKey = "xtrader:hist:path:queue:v1";
constexpr const char* kDlqKey = "xtrader:hist:path:dlq:v1";
constexpr const char* kMetaPattern = "xtrader:hist:dataset:*:meta";
constexpr const char* kSourcePattern = "xtrader:hist:source:*";

enum class nav {
    stay,
    back,
    root,
    quit,
};

struct launch_options {
    std::filesystem::path data_root = "/data/x_trader_data";
    std::string redis_host = "127.0.0.1";
    int redis_port = 6379;
};

std::string to_lower(std::string input) {
    std::transform(input.begin(), input.end(), input.begin(), [] (unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return input;
}

nav parse_nav(const std::string& input) {
    const std::string normalized = to_lower(input);
    if (normalized == "u" || normalized == "up" || normalized == "back") {
        return nav::back;
    }
    if (normalized == "h" || normalized == "home" || normalized == "root") {
        return nav::root;
    }
    if (normalized == "q" || normalized == "quit" || normalized == "exit") {
        return nav::quit;
    }
    return nav::stay;
}

std::string sanitize_token(const std::string& value) {
    std::string out;
    out.reserve(value.size());

    for (unsigned char c : value) {
        if (std::isalnum(c) != 0) {
            out.push_back(static_cast<char>(std::tolower(c)));
        } else {
            out.push_back('_');
        }
    }

    while (!out.empty() && out.back() == '_') {
        out.pop_back();
    }

    if (out.empty()) {
        return "unknown";
    }
    return out;
}

std::string build_dataset_id(const seastar::xtrader::dataset_manifest& ds) {
    return sanitize_token(ds.symbol) + "__"
        + sanitize_token(ds.exchange) + "__"
        + sanitize_token(ds.timeframe) + "__"
        + sanitize_token(ds.active_file_path.generic_string());
}

std::string build_meta_key(const std::string& dataset_id) {
    return "xtrader:hist:dataset:" + dataset_id + ":meta";
}

bool ensure_redis(seastar::xtrader::redis_sync_client& redis, const launch_options& options) {
    if (redis.is_connected()) {
        return true;
    }

    if (!redis.connect(options.redis_host, options.redis_port)) {
        std::cout << "Failed to connect redis: " << options.redis_host << ':' << options.redis_port << std::endl;
        return false;
    }

    return true;
}

bool write_dataset_to_redis(
    seastar::xtrader::redis_sync_client& redis,
    const seastar::xtrader::dataset_manifest& ds,
    bool enqueue_latest) {

    if (ds.active_file_path.empty()) {
        return false;
    }

    const std::string path = ds.active_file_path.generic_string();
    const std::string dataset_id = build_dataset_id(ds);

    const bool meta_ok = redis.set(build_meta_key(dataset_id), path);
    const std::string source_key = "xtrader:hist:source:" + (ds.source.empty() ? "taobao_csv" : ds.source);
    const long long source_push = redis.lpush(source_key, path);

    if (enqueue_latest) {
        const bool latest_ok = redis.set(kLatestKey, path);
        const long long queue_push = redis.lpush(kQueueKey, path);
        if (!latest_ok || queue_push <= 0) {
            return false;
        }
    }

    return meta_ok && source_push > 0;
}

std::vector<std::string> load_dataset_paths(
    seastar::xtrader::redis_sync_client& redis) {

    std::vector<std::string> paths;

    auto keys = redis.keys(kMetaPattern);
    std::sort(keys.begin(), keys.end());
    for (const auto& key : keys) {
        const auto value = redis.get(key);
        if (!value.empty()) {
            paths.push_back(value);
        }
    }

    if (paths.empty()) {
        paths = redis.lrange(kQueueKey, 0, -1);
    }

    if (paths.empty()) {
        const auto source_keys = redis.keys(kSourcePattern);
        for (const auto& source_key : source_keys) {
            auto values = redis.lrange(source_key, 0, -1);
            paths.insert(paths.end(), values.begin(), values.end());
        }
    }

    std::sort(paths.begin(), paths.end());
    paths.erase(std::unique(paths.begin(), paths.end()), paths.end());
    return paths;
}

void do_create_latest(
    seastar::xtrader::redis_sync_client& redis,
    const launch_options& options) {

    seastar::xtrader::historical_data_manager manager(options.data_root);
    std::string warning;
    auto datasets = manager.scan_datasets(&warning);

    if (datasets.empty()) {
        std::cout << (warning.empty() ? "No dataset found." : warning) << std::endl;
        return;
    }

    const seastar::xtrader::dataset_manifest* latest = nullptr;
    std::filesystem::file_time_type latest_time;

    for (const auto& ds : datasets) {
        if (ds.active_file_path.empty() || !std::filesystem::exists(ds.active_file_path)) {
            continue;
        }

        std::error_code ec;
        auto t = std::filesystem::last_write_time(ds.active_file_path, ec);
        if (ec) {
            continue;
        }

        if (latest == nullptr || t > latest_time) {
            latest = &ds;
            latest_time = t;
        }
    }

    if (latest == nullptr) {
        std::cout << "No valid dataset with active file found." << std::endl;
        return;
    }

    if (!write_dataset_to_redis(redis, *latest, true)) {
        std::cout << "Failed to write latest dataset redis entry." << std::endl;
        return;
    }

    std::cout << "Latest dataset redis entry created: " << build_dataset_id(*latest) << std::endl;
}

void do_rebuild(
    seastar::xtrader::redis_sync_client& redis,
    const launch_options& options) {

    seastar::xtrader::historical_data_manager manager(options.data_root);
    std::string warning;
    auto datasets = manager.scan_datasets(&warning);

    if (datasets.empty()) {
        std::cout << (warning.empty() ? "No dataset found." : warning) << std::endl;
        return;
    }

    std::size_t written = 0;
    for (const auto& ds : datasets) {
        if (write_dataset_to_redis(redis, ds, false)) {
            ++written;
        }
    }

    std::cout << "Rebuild completed, redis dataset meta written: " << written << std::endl;
}

void do_summary(seastar::xtrader::redis_sync_client& redis) {
    const auto paths = load_dataset_paths(redis);
    const auto latest = redis.get(kLatestKey);
    const auto queue_depth = redis.llen(kQueueKey);
    const auto dlq_depth = redis.llen(kDlqKey);

    std::cout << "Historical dataset meta count: " << paths.size() << std::endl;
    std::cout << "Latest dataset path: " << (latest.empty() ? "(none)" : latest) << std::endl;
    std::cout << "Queue depth: " << queue_depth << std::endl;
    std::cout << "DLQ depth: " << dlq_depth << std::endl;

    for (std::size_t i = 0; i < std::min<std::size_t>(paths.size(), 10); ++i) {
        std::cout << "  [" << (i + 1) << "] " << paths[i] << std::endl;
    }
}

void do_detail(seastar::xtrader::redis_sync_client& redis, const std::string& dataset_id) {
    if (dataset_id.empty()) {
        std::cout << "dataset_id is empty." << std::endl;
        return;
    }

    std::string value = redis.get(build_meta_key(dataset_id));
    if (!value.empty()) {
        std::cout << value << std::endl;
        return;
    }

    if (dataset_id.rfind("xtrader:", 0) == 0) {
        value = redis.get(dataset_id);
        if (!value.empty()) {
            std::cout << value << std::endl;
            return;
        }
    }

    try {
        const int idx = std::stoi(dataset_id);
        if (idx > 0) {
            const auto queue_values = redis.lrange(kQueueKey, idx - 1, idx - 1);
            if (!queue_values.empty()) {
                std::cout << queue_values.front() << std::endl;
                return;
            }
        }
    } catch (const std::exception&) {
    }

    std::cout << "dataset meta not found: " << dataset_id << std::endl;
}

void do_clear_meta(seastar::xtrader::redis_sync_client& redis) {
    const auto keys = redis.keys(kMetaPattern);
    for (const auto& key : keys) {
        (void)redis.del(key);
    }
    (void)redis.del(kLatestKey);
    std::cout << "Cleared historical dataset meta keys: " << keys.size() << std::endl;
}

void do_clear_queue(seastar::xtrader::redis_sync_client& redis) {
    (void)redis.del(kQueueKey);
    (void)redis.del(kDlqKey);
    (void)redis.del(kLatestKey);
    std::cout << "Cleared queue, dlq and latest pointer." << std::endl;
}

nav run_historical_redis_menu(launch_options& options, seastar::xtrader::redis_sync_client& redis) {
    while (true) {
        std::cout << "\n[Historical Redis Menu] data_root=" << options.data_root.generic_string()
            << ", redis=" << options.redis_host << ':' << options.redis_port << "\n"
            << "  1) Create latest dataset redis entry from latest import result\n"
            << "  2) Rebuild dataset redis index from data root\n"
            << "  3) Read dataset index summary\n"
            << "  4) Read one dataset detail by dataset_id\n"
            << "  5) Show redis queue/status\n"
            << "  6) Clear dataset index/meta\n"
            << "  7) Clear queue/dlq/latest pointer\n"
            << "  8) Set data root\n"
            << "  9) Set redis host\n"
            << " 10) Set redis port\n"
            << "  u) Up one level\n"
            << "  h) Back to root\n"
            << "  q) Quit\n"
            << "Select: ";

        std::string input;
        if (!std::getline(std::cin, input)) {
            return nav::quit;
        }

        const auto menu_nav = parse_nav(input);
        if (menu_nav != nav::stay) {
            return menu_nav;
        }

        const auto normalized = to_lower(input);
        if (!ensure_redis(redis, options)) {
            continue;
        }

        if (normalized == "1" || normalized == "create") {
            do_create_latest(redis, options);
            continue;
        }
        if (normalized == "2" || normalized == "rebuild") {
            do_rebuild(redis, options);
            continue;
        }
        if (normalized == "3" || normalized == "read" || normalized == "summary") {
            do_summary(redis);
            continue;
        }
        if (normalized == "4" || normalized == "detail") {
            std::cout << "Enter dataset_id: ";
            std::string dataset_id;
            if (!std::getline(std::cin, dataset_id)) {
                return nav::quit;
            }
            const auto inner_nav = parse_nav(dataset_id);
            if (inner_nav == nav::quit || inner_nav == nav::root) {
                return inner_nav;
            }
            if (inner_nav == nav::back) {
                continue;
            }
            do_detail(redis, dataset_id);
            continue;
        }
        if (normalized == "5" || normalized == "status") {
            do_summary(redis);
            continue;
        }
        if (normalized == "6" || normalized == "clear-meta") {
            do_clear_meta(redis);
            continue;
        }
        if (normalized == "7" || normalized == "clear-queue") {
            do_clear_queue(redis);
            continue;
        }
        if (normalized == "8" || normalized == "path") {
            std::cout << "Enter historical redis data root path (empty keeps current): ";
            std::string value;
            if (!std::getline(std::cin, value)) {
                return nav::quit;
            }
            if (!value.empty()) {
                options.data_root = value;
            }
            continue;
        }
        if (normalized == "9" || normalized == "host") {
            std::cout << "Enter redis host: ";
            std::string value;
            if (!std::getline(std::cin, value)) {
                return nav::quit;
            }
            if (!value.empty()) {
                options.redis_host = value;
                redis.disconnect();
            }
            continue;
        }
        if (normalized == "10" || normalized == "port") {
            std::cout << "Enter redis port: ";
            std::string value;
            if (!std::getline(std::cin, value)) {
                return nav::quit;
            }
            if (!value.empty()) {
                try {
                    options.redis_port = std::stoi(value);
                    redis.disconnect();
                } catch (const std::exception&) {
                    std::cout << "Invalid redis port." << std::endl;
                }
            }
            continue;
        }

        std::cout << "Invalid input." << std::endl;
    }
}

void run_placeholder_menu(const char* title) {
    while (true) {
        std::cout << "\n[" << title << " Menu]" << std::endl;
        std::cout << "  This menu is scaffolded in Seastar migration stage." << std::endl;
        std::cout << "  Implemented next steps will replace this placeholder." << std::endl;
        std::cout << "  u) Up one level\n  h) Back to root\n  q) Quit\nSelect: ";

        std::string input;
        if (!std::getline(std::cin, input)) {
            return;
        }
        const auto n = parse_nav(input);
        if (n == nav::quit || n == nav::back || n == nav::root) {
            return;
        }

        std::cout << "Invalid input." << std::endl;
    }
}

void run_root_menu() {
    launch_options options;
    seastar::xtrader::redis_sync_client redis;

    while (true) {
        std::cout << "\n[Root Menu]\n"
            << "  l) Live\n"
            << "  r) Replay\n"
            << "  i) Import\n"
            << "  b) Data Brief\n"
            << "  d) Historical Redis\n"
            << "  t) Heartbeat Test\n"
            << "  q) Quit\n"
            << "Select run mode: ";

        std::string input;
        if (!std::getline(std::cin, input)) {
            break;
        }

        const auto normalized = to_lower(input);
        if (normalized == "q" || normalized == "quit" || normalized == "exit") {
            break;
        }

        if (normalized == "d" || normalized == "redis" || normalized == "historical-redis") {
            const auto n = run_historical_redis_menu(options, redis);
            if (n == nav::quit) {
                break;
            }
            continue;
        }

        if (normalized == "l" || normalized == "live") {
            run_placeholder_menu("Live");
            continue;
        }
        if (normalized == "r" || normalized == "replay") {
            run_placeholder_menu("Replay");
            continue;
        }
        if (normalized == "i" || normalized == "import") {
            run_placeholder_menu("Import");
            continue;
        }
        if (normalized == "b" || normalized == "brief" || normalized == "data-brief") {
            run_placeholder_menu("Data Brief");
            continue;
        }
        if (normalized == "t" || normalized == "heartbeat") {
            run_placeholder_menu("Heartbeat Test");
            continue;
        }

        std::cout << "Invalid input. Please enter l/r/i/b/d/t/q." << std::endl;
    }

    redis.disconnect();
}

} // namespace

int main(int ac, char** av) {
    seastar::app_template app;

    return app.run(ac, av, [] {
        return seastar::async([] {
            run_root_menu();
            return 0;
        });
    });
}
