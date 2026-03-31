#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

namespace {

enum class BackendMode {
    Posix,
    Uring,
    All,
};

BackendMode parse_backend(int argc, char** argv) {
    BackendMode mode = BackendMode::All;
    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        if (arg == "--backend=posix") {
            mode = BackendMode::Posix;
        } else if (arg == "--backend=uring") {
            mode = BackendMode::Uring;
        } else if (arg == "--backend=all") {
            mode = BackendMode::All;
        } else {
            std::cerr << "unknown arg: " << arg << '\n';
            std::exit(2);
        }
    }
    return mode;
}

const char* backend_name(BackendMode mode) {
    switch (mode) {
        case BackendMode::Posix:
            return "posix";
        case BackendMode::Uring:
            return "uring";
        case BackendMode::All:
            return "all";
    }
    return "unknown";
}

int run_one(const std::filesystem::path& exe_dir, const std::string& binary, const char* backend, std::string_view extra_args = {}) {
    const auto exe = exe_dir / binary;
    const std::string cmd = "cd " + exe_dir.string() + " && ./" + binary + " --backend=" + backend
        + (extra_args.empty() ? "" : " " + std::string(extra_args));
    std::cout << "[mt_all_tests] START " << binary << " backend=" << backend;
    if (!extra_args.empty()) {
        std::cout << " args=" << extra_args;
    }
    std::cout << std::endl;
    const int rc = std::system(cmd.c_str());
    if (rc == 0) {
        std::cout << "[mt_all_tests] PASS  " << binary << " backend=" << backend;
        if (!extra_args.empty()) {
            std::cout << " args=" << extra_args;
        }
        std::cout << std::endl;
    } else {
        std::cout << "[mt_all_tests] FAIL  " << binary << " backend=" << backend;
        if (!extra_args.empty()) {
            std::cout << " args=" << extra_args;
        }
        std::cout << " rc=" << rc << std::endl;
    }
    return rc;
}

int run_backend(const std::filesystem::path& exe_dir, const char* backend) {
    struct TestInvocation {
        std::string binary;
        std::string extra_args;
    };
    const std::vector<TestInvocation> tests = {
        {"buffer_mgr_mt_smoke_test", ""},
        {"buffer_mgr_mt_runtime_test", ""},
        {"buffer_mgr_mt_system_test", ""},
        {"buffer_mgr_mt_eviction_test", ""},
        {"buffer_mgr_mt_multiworker_eviction_test", ""},
        {"buffer_mgr_mt_multiworker_progress_test", "--workers=2"},
        {"buffer_mgr_mt_multiworker_progress_test", "--workers=4"},
    };

    for (const auto& test : tests) {
        const int rc = run_one(exe_dir, test.binary, backend, test.extra_args);
        if (rc != 0) {
            return 1;
        }
    }
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    const auto mode = parse_backend(argc, argv);
    const std::filesystem::path exe_dir = std::filesystem::absolute(argv[0]).parent_path();

    std::cout << "[mt_all_tests] backend=" << backend_name(mode) << std::endl;

    int failures = 0;
    if (mode == BackendMode::Posix || mode == BackendMode::All) {
        failures = run_backend(exe_dir, "posix");
        if (failures != 0) {
            std::cout << "[mt_all_tests] FAILURES=" << failures << std::endl;
            return 1;
        }
    }
    if (mode == BackendMode::Uring || mode == BackendMode::All) {
        failures = run_backend(exe_dir, "uring");
        if (failures != 0) {
            std::cout << "[mt_all_tests] FAILURES=" << failures << std::endl;
            return 1;
        }
    }

    std::cout << "[mt_all_tests] ALL PASS" << std::endl;
    return 0;
}
