#include <asm/unistd.h>
#include <fcntl.h>
#include <linux/perf_event.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

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

// Helper function
double read_counter(PerfEventItem* event) {
    double correction = (double)(event->data.time_enabled - event->prev.time_enabled) /
                        (double)(event->data.time_running - event->prev.time_running);
    return (double)(event->data.value - event->prev.value) * correction;
}

void register_counter(PerfEvent* pevent, const char* name, uint64_t type, uint64_t config, EventDomain domain) {
    if (pevent->count >= MAX_EVENTS)
        return;

    strncpy(pevent->names[pevent->count], name, NAME_LEN - 1);
    PerfEventItem* e = &pevent->events[pevent->count];
    memset(&e->pe, 0, sizeof(struct perf_event_attr));

    e->pe.type = type;
    e->pe.size = sizeof(struct perf_event_attr);
    e->pe.config = config;
    e->pe.disabled = 1;
    e->pe.inherit = 1;
    e->pe.exclude_user = !(domain & USER);
    e->pe.exclude_kernel = !(domain & KERNEL);
    e->pe.exclude_hv = !(domain & HYPERVISOR);
    e->pe.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;

    e->fd = syscall(__NR_perf_event_open, &e->pe, 0, -1, -1, 0);
    if (e->fd < 0) {
        perror("perf_event_open failed");
        exit(1);
    }

    pevent->count++;
}

void start_counters(PerfEvent* pevent) {
    for (int i = 0; i < pevent->count; i++) {
        ioctl(pevent->events[i].fd, PERF_EVENT_IOC_RESET, 0);
        ioctl(pevent->events[i].fd, PERF_EVENT_IOC_ENABLE, 0);
        ssize_t sz = read(pevent->events[i].fd, &pevent->events[i].prev, sizeof(uint64_t) * 3);
        if (sz != sizeof(uint64_t) * 3) {
            perror("read prev failed");
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &pevent->startTime);
}

void stop_counters(PerfEvent* pevent) {
    clock_gettime(CLOCK_MONOTONIC, &pevent->stopTime);
    for (int i = 0; i < pevent->count; i++) {
        ssize_t sz = read(pevent->events[i].fd, &pevent->events[i].data, sizeof(uint64_t) * 3);
        if (sz != sizeof(uint64_t) * 3) {
            perror("read data failed");
        }
        ioctl(pevent->events[i].fd, PERF_EVENT_IOC_DISABLE, 0);
    }
}

double get_duration(PerfEvent* pevent) {
    return (pevent->stopTime.tv_sec - pevent->startTime.tv_sec) +
           (pevent->stopTime.tv_nsec - pevent->startTime.tv_nsec) / 1e9;
}

double get_counter(PerfEvent* pevent, const char* name) {
    for (int i = 0; i < pevent->count; i++) {
        if (strcmp(name, pevent->names[i]) == 0) {
            return read_counter(&pevent->events[i]);
        }
    }
    return -1.0;
}

double get_IPC(PerfEvent* pevent) {
    return get_counter(pevent, "instructions") / get_counter(pevent, "cycles");
}

double get_CPUs(PerfEvent* pevent) {
    return get_counter(pevent, "task-clock") / (get_duration(pevent) * 1e9);
}

double get_GHz(PerfEvent* pevent) {
    return get_counter(pevent, "cycles") / get_counter(pevent, "task-clock");
}

void print_report(PerfEvent* pevent, uint64_t normalization, int table) {
    if (pevent->count == 0)
        return;


    if (table) {
        printf("%-20s | %-12s\n", "Metric", "Value");
        printf("---------------------|--------------\n");

        for (int i = 0; i < pevent->count; i++) {
            double val = read_counter(&pevent->events[i]) / (double)normalization;
            printf("%-20s | %12.2f\n", pevent->names[i], val);
        }

        printf("%-20s | %12lu\n", "scale", normalization);
        printf("%-20s | %12.2f\n", "IPC", get_IPC(pevent));
        printf("%-20s | %12.2f\n", "CPUs", get_CPUs(pevent));
        printf("%-20s | %12.2f\n", "GHz", get_GHz(pevent));
    } else {

        for (int i = 0; i < pevent->count; i++) {
            double val = read_counter(&pevent->events[i]) / (double)normalization;
            printf("%s=%.2f\n", pevent->names[i], val);
        }

        printf("%s=%lu\n", "scale", normalization);
        printf("%s=%.2f\n", "IPC", get_IPC(pevent));
        printf("%s=%.2f\n", "CPUs", get_CPUs(pevent));
        printf("%s=%.2f\n", "GHz", get_GHz(pevent));
    }
}

void cleanup(PerfEvent* pevent) {
    for (int i = 0; i < pevent->count; i++) {
        close(pevent->events[i].fd);
    }
}
