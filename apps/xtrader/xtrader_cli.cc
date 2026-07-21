/*
 * X-Trader CLI - Main entry point for X-Trader Seastar trading engine
 */
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/xtrader/data_importer.hh>
#include <seastar/xtrader/historical_data_manager.hh>
#include <seastar/xtrader/redis_sync_client.hh>
#include <seastar/xtrader/version.hh>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>

namespace {

std::string to_lower(std::string input) {
    std::transform(input.begin(), input.end(), input.begin(), [] (unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return input;
}

void show_config_info() {
    std::cout << "\n========== Configuration Info ==========" << std::endl;
    std::cout << "Version:    " << seastar::xtrader::version << std::endl;
    std::cout << "Build:      " << seastar::xtrader::build_type << std::endl;
    std::cout << "Git Commit: " << seastar::xtrader::git_commit << std::endl;
    std::cout << "Git Branch: " << seastar::xtrader::git_branch << std::endl;
    std::cout << "Build Time: " << seastar::xtrader::build_timestamp << std::endl;
    std::cout << "========================================" << std::endl;

    // 配置文件路径信息
    std::cout << "\n--- Config File Paths ---" << std::endl;
    std::vector<std::string> config_paths = {
        "/etc/xtrader/xtrader.ini",
        "/home/xtrader/config/xtrader.ini",
        "./config/xtrader.ini",
        "./xtrader.ini"
    };
    for (const auto& path : config_paths) {
        if (std::filesystem::exists(path)) {
            std::cout << "[FOUND] " << path << std::endl;
        } else {
            std::cout << "[MISSING] " << path << std::endl;
        }
    }

    // 数据目录
    std::cout << "\n--- Data Directories ---" << std::endl;
    std::cout << "Default Data Root: /data/x_trader_data" << std::endl;
    std::cout << "========================================" << std::endl;
}

void run_heartbeat_test() {
    std::cout << "\n[Heartbeat Test]" << std::endl;
    auto start = std::chrono::steady_clock::now();
    seastar::xtrader::redis_sync_client redis;
    if (!redis.connect("127.0.0.1", 6379)) {
        std::cout << "[FAIL] Cannot connect to Redis" << std::endl;
        return;
    }
    auto latency = std::chrono::steady_clock::now() - start;
    std::cout << "[OK] Connected to Redis in "
              << std::chrono::duration<double, std::milli>(latency).count()
              << " ms" << std::endl;

    // Test SET/GET
    bool set_ok = redis.set("xtrader:heartbeat:test", "ok");
    if (set_ok) {
        std::string val = redis.get("xtrader:heartbeat:test");
        std::cout << "[OK] SET/GET test: " << val << std::endl;
    } else {
        std::cout << "[FAIL] SET failed" << std::endl;
    }

    redis.disconnect();
}

void run_root_menu() {
    std::string data_root = "/data/x_trader_data";
    std::string import_source_path;
    seastar::xtrader::import_source_type import_source_type = seastar::xtrader::import_source_type::taobao_csv;
    seastar::xtrader::import_mode import_mode = seastar::xtrader::import_mode::incremental;
    std::string import_config_path;

    while (true) {
        std::cout << "\n========== X-Trader CLI ==========" << std::endl;
        std::cout << "1) Live" << std::endl;
        std::cout << "2) Replay" << std::endl;
        std::cout << "3) Import" << std::endl;
        std::cout << "4) Data Brief" << std::endl;
        std::cout << "5) Config Info" << std::endl;
        std::cout << "6) Heartbeat Test" << std::endl;
        std::cout << "q) Quit" << std::endl;
        std::cout << "==================================" << std::endl;
        std::cout << "Select: ";

        std::string input;
        std::getline(std::cin, input);
        input = to_lower(input);

        if (input == "1") {
            std::cout << "[TODO] Live mode" << std::endl;
        } else if (input == "2") {
            std::cout << "[TODO] Replay mode" << std::endl;
        } else if (input == "3") {
            // Import menu
            while (true) {
                std::cout << "\n[Import Menu]" << std::endl;
                std::cout << "  source_path: " << (import_source_path.empty() ? "(empty)" : import_source_path) << std::endl;
                std::cout << "  source_type: " << (import_source_type == seastar::xtrader::import_source_type::taobao_csv ? "taobao_csv" : "ctp_clean_csv") << std::endl;
                std::cout << "  mode: " << (import_mode == seastar::xtrader::import_mode::incremental ? "incremental" : "overwrite") << std::endl;
                std::cout << "  config_path: " << (import_config_path.empty() ? "(default)" : import_config_path) << std::endl;
                std::cout << "  1) Set source path" << std::endl;
                std::cout << "  2) Set source type" << std::endl;
                std::cout << "  3) Set import mode" << std::endl;
                std::cout << "  4) Set config path" << std::endl;
                std::cout << "  5) Run import" << std::endl;
                std::cout << "  u) Up" << std::endl;
                std::cout << "Select: ";

                std::getline(std::cin, input);
                input = to_lower(input);

                if (input == "1") {
                    std::cout << "Enter source path: ";
                    std::getline(std::cin, import_source_path);
                    std::cout << "source_path set to " << import_source_path << std::endl;
                } else if (input == "2") {
                    std::cout << "Source types:\n  1) taobao_csv\n  2) ctp_clean_csv\nSelect: ";
                    std::getline(std::cin, input);
                    if (input == "1" || input == "taobao" || input == "taobao_csv") {
                        import_source_type = seastar::xtrader::import_source_type::taobao_csv;
                        std::cout << "source_type set to taobao_csv" << std::endl;
                    } else if (input == "2" || input == "ctp" || input == "ctp_clean_csv") {
                        import_source_type = seastar::xtrader::import_source_type::ctp_clean_csv;
                        std::cout << "source_type set to ctp_clean_csv" << std::endl;
                    } else {
                        std::cerr << "[ERROR] Unknown source type" << std::endl;
                    }
                } else if (input == "3") {
                    std::cout << "Import modes:\n  1) incremental\n  2) overwrite\nSelect: ";
                    std::getline(std::cin, input);
                    if (input == "1" || input == "incremental" || input == "inc") {
                        import_mode = seastar::xtrader::import_mode::incremental;
                        std::cout << "import_mode set to incremental" << std::endl;
                    } else if (input == "2" || input == "overwrite" || input == "full") {
                        import_mode = seastar::xtrader::import_mode::overwrite;
                        std::cout << "import_mode set to overwrite" << std::endl;
                    } else {
                        std::cerr << "[ERROR] Unknown import mode" << std::endl;
                    }
                } else if (input == "4") {
                    std::cout << "Enter config path (empty for default): ";
                    std::getline(std::cin, import_config_path);
                    if (import_config_path.empty()) {
                        std::cout << "config_path reset to default" << std::endl;
                    } else {
                        std::cout << "config_path set to " << import_config_path << std::endl;
                    }
                } else if (input == "5") {
                    if (import_source_path.empty()) {
                        std::cerr << "[ERROR] source_path not set" << std::endl;
                        continue;
                    }
                    if (!std::filesystem::exists(import_source_path)) {
                        std::cerr << "[ERROR] source_path does not exist: " << import_source_path << std::endl;
                        continue;
                    }

                    seastar::xtrader::import_options opts;
                    opts.source_path = import_source_path;
                    opts.source_type = import_source_type;
                    opts.mode = import_mode;
                    if (!import_config_path.empty()) {
                        opts.config_path = import_config_path;
                    }

                    std::cout << "Starting import..." << std::endl;
                    seastar::xtrader::data_importer importer;
                    auto result = importer.execute(opts);

                    std::cout << "[OK] Import " << (result.success ? "succeeded" : "failed") << std::endl;
                    std::cout << "  Files: " << result.scanned_files << " scanned, "
                              << result.success_files << " success, "
                              << result.skipped_files << " skipped, "
                              << result.failed_files << " failed" << std::endl;
                    std::cout << "  Rows: " << result.rows_added << " added, "
                              << result.rows_overwritten << " overwritten, "
                              << result.rows_skipped << " skipped" << std::endl;
                    if (!result.message.empty()) {
                        std::cout << "  Message: " << result.message << std::endl;
                    }
                } else if (input == "u" || input == "back" || input == "up") {
                    break;
                } else {
                    std::cerr << "[ERROR] Unknown command" << std::endl;
                }
            }
        } else if (input == "4") {
            // Data brief
            std::cout << "\n[Data Brief]" << std::endl;
            seastar::xtrader::historical_data_manager manager(data_root);
            std::string warning;
            auto datasets = manager.scan_datasets(&warning);
            if (!warning.empty()) {
                std::cout << "[WARN] " << warning << std::endl;
            }
            std::cout << "Found " << datasets.size() << " datasets:" << std::endl;
            for (const auto& ds : datasets) {
                std::cout << "  " << ds.exchange << "/" << ds.symbol
                          << " " << ds.timeframe
                          << " (" << ds.row_count << " records)" << std::endl;
            }
        } else if (input == "5") {
            show_config_info();
        } else if (input == "6") {
            run_heartbeat_test();
        } else if (input == "q" || input == "quit" || input == "exit") {
            std::cout << "Goodbye!" << std::endl;
            break;
        } else {
            std::cout << "Invalid option" << std::endl;
        }
    }
}

} // namespace

int main(int ac, char** av) {
    std::cout << "========================================" << std::endl;
    std::cout << "X-Trader Seastar Trading Engine v"
              << seastar::xtrader::version << std::endl;
    std::cout << "Build: " << seastar::xtrader::build_type
              << " | Git: " << seastar::xtrader::git_commit << std::endl;
    std::cout << "========================================" << std::endl;

    seastar::app_template app;

    return app.run(ac, av, [] {
        return seastar::async([] {
            run_root_menu();
        });
    });
}
