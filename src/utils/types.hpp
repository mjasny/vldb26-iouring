#pragma once

#include <iostream>


enum class SetupMode {
    DEFAULT,
    SQPOLL,
    COOP_TASKRUN,
    DEFER_TASKRUN,
    HYBRID_IOPOLL,
};

std::ostream& operator<<(std::ostream& os, const SetupMode& arg);
std::istream& operator>>(std::istream& is, SetupMode& mode);

enum class IssueMode {
    CHUNK,
    BUDGET,
    TIMED,
};

std::ostream& operator<<(std::ostream& os, const IssueMode& arg);
std::istream& operator>>(std::istream& is, IssueMode& mode);
