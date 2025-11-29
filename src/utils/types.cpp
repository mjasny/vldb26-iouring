#include "types.hpp"


std::ostream& operator<<(std::ostream& os, const SetupMode& arg) {
    switch (arg) {
        case SetupMode::DEFAULT:
            os << "default";
            break;
        case SetupMode::SQPOLL:
            os << "sqpoll";
            break;
        case SetupMode::DEFER_TASKRUN:
            os << "defer";
            break;
        case SetupMode::COOP_TASKRUN:
            os << "coop";
            break;
        case SetupMode::HYBRID_IOPOLL:
            os << "hybrid";
            break;
    }
    return os;
}

std::istream& operator>>(std::istream& is, SetupMode& mode) {
    std::string token;
    is >> token;

    if (token == "default") {
        mode = SetupMode::DEFAULT;
    } else if (token == "sqpoll") {
        mode = SetupMode::SQPOLL;
    } else if (token == "defer") {
        mode = SetupMode::DEFER_TASKRUN;
    } else if (token == "coop") {
        mode = SetupMode::COOP_TASKRUN;
    } else if (token == "hybrid") {
        mode = SetupMode::HYBRID_IOPOLL;
    } else {
        throw std::invalid_argument("Invalid input for SetupMode: " + token);
    }
    return is;
}


std::ostream& operator<<(std::ostream& os, const IssueMode& arg) {
    switch (arg) {
        case IssueMode::CHUNK:
            os << "chunk";
            break;
        case IssueMode::BUDGET:
            os << "budget";
            break;
        case IssueMode::TIMED:
            os << "timed";
            break;
    }
    return os;
}

std::istream& operator>>(std::istream& is, IssueMode& mode) {
    std::string token;
    is >> token;

    if (token == "chunk") {
        mode = IssueMode::CHUNK;
    } else if (token == "budget") {
        mode = IssueMode::BUDGET;
    } else if (token == "timed") {
        mode = IssueMode::TIMED;
    } else {
        throw std::invalid_argument("Invalid input for IssueMode: " + token);
    }
    return is;
}
