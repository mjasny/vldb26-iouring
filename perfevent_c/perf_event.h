#ifndef PERF_EVENT_H
#define PERF_EVENT_H

#include <linux/perf_event.h>
#include <stdint.h>
#include <time.h>

#define MAX_EVENTS 32
#define NAME_LEN 64

typedef enum {
    USER = 0b1,
    KERNEL = 0b10,
    HYPERVISOR = 0b100,
    ALL = 0b111
} EventDomain;

typedef struct {
    uint64_t value;
    uint64_t time_enabled;
    uint64_t time_running;
    uint64_t id;
} read_format;

typedef struct {
    struct perf_event_attr pe;
    int fd;
    read_format prev;
    read_format data;
} PerfEventItem;

typedef struct {
    PerfEventItem events[MAX_EVENTS];
    char names[MAX_EVENTS][NAME_LEN];
    int count;
    struct timespec startTime, stopTime;
} PerfEvent;

void register_counter(PerfEvent* pevent, const char* name, uint64_t type, uint64_t config, EventDomain domain);
void start_counters(PerfEvent* pevent);
void stop_counters(PerfEvent* pevent);
void cleanup(PerfEvent* pevent);
double get_counter(PerfEvent* pevent, const char* name);
double get_duration(PerfEvent* pevent);
double get_IPC(PerfEvent* pevent);
double get_CPUs(PerfEvent* pevent);
double get_GHz(PerfEvent* pevent);
void print_report(PerfEvent* pevent, uint64_t normalization, int table);

#endif // PERF_EVENT_H
