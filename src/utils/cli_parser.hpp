#pragma once

#include "utils/my_asserts.hpp"
#include "utils/nostd.hpp"

#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>


namespace cli {

class Parser {
    struct ArgValue {
        std::string value;
        bool parsed = false;
    };
    std::map<std::string, ArgValue> pairs;
    std::stringstream ss;

public:
    enum Options {
        required,
        optional,
    };

    Parser(int argc, char** argv) {
        ss << argv[0] << "\n";
        for (int i = 1; i < argc; i += 2) {
            pairs[argv[i]].value = argv[i + 1];
        }
    }

    void print() {
        std::cout << ss.rdbuf();
    }

    void check_unparsed(bool throw_error = true) {
        std::stringstream ss;
        size_t n = 0;
        ss << "Unparsed parameters: ";
        for (auto& [k, v] : pairs) {
            if (!v.parsed) {
                if (n++ > 0) {
                    ss << ", ";
                }
                ss << k;
            }
        }
        if (n > 0) {
            ss << " (total=" << n << ")\n";
            if (throw_error) {
                throw std::runtime_error(ss.str());
            } else {
                std::cerr << ss.str();
            }
        }
    }

    template <typename T>
    void parse(std::string param, T& value, Options options = Options::required) {
        if (pairs.find(param) == pairs.end()) {
            if (options == Options::optional) {
                add_print(param, value);
                return;
            }
            std::stringstream ss;
            ss << "Parameter " << param << " is missing.";
            throw std::invalid_argument(ss.str());
        }

        auto& arg = pairs.at(param);
        parse_arg(arg.value, value);
        arg.parsed = true;
        add_print(param, value);
    }

    template <typename T>
    void parse(std::string param, std::vector<T>& values, Options options = Options::required) {
        // if (!pairs.contains(param)) { // C++20
        if (pairs.find(param) == pairs.end()) {
            if (options == Options::optional) {
                add_print(param, values);
                return;
            }
            std::stringstream ss;
            ss << "Parameter " << param << " is missing.";
            throw std::invalid_argument(ss.str());
        }


        values.clear();

        auto& arg = pairs.at(param);
        std::istringstream iss{arg.value};
        std::string item;
        while (std::getline(iss, item, ',')) {
            T value;
            parse_arg(item, value);
            values.push_back(value);
        }
        arg.parsed = true;
        add_print(param, values);
    }


private:
    template <typename T>
    void parse_arg(std::string& arg, T& value) {
        std::istringstream ss{arg};
        ss >> value;
    }

    template <typename T>
    void add_print(std::string param, T& value) {
        ss << "    " << param << "=" << value << "\n";
    }

    template <typename T>
    void add_print(std::string param, std::vector<T>& values) {
        ss << "    " << param << "=";
        for (size_t i = 0; i < values.size(); ++i) {
            if (i > 0) {
                ss << ",";
            }
            ss << values[i];
        }
        ss << "\n";
    }
};


template <>
void Parser::parse_arg(std::string& arg, char& value) {
    ensure(arg.size() == 1);
    value = arg[0];
}

template <>
void Parser::parse_arg(std::string& arg, uint8_t& value) {
    value = nostd::stou8(arg);
}

template <>
void Parser::parse_arg(std::string& arg, bool& value) {
    std::transform(arg.begin(), arg.end(), arg.begin(), ::tolower);
    std::istringstream is(arg);
    is >> std::boolalpha >> value;
}

} // namespace cli
