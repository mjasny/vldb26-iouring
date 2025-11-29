import sys
import argparse
import os
import re
import time


def read_file(path, default=None):
    try:
        with open(path) as f:
            return f.read().strip()
    except FileNotFoundError:
        if default is not None:
            return default
        raise


def get_irq_affinity(name):
    irq_map = {}

    with open('/proc/interrupts') as f:
        # header
        line = next(f)
        parts = line.strip().split()
        n_cpus = len(parts)

        for line in f:
            if name not in line:
                continue

            parts = line.strip().split()
            irqs = list(map(int, parts[1:n_cpus+1]))

            # 1 CPU handled for this irq
            # assert (sum(1 for x in irqs if x != 0) <= 1)
            irq_count = next((x for x in irqs if x != 0), -1)

            irq = parts[0].rstrip(':')
            # Extract queue name like nvme8q7
            # mlx5_comp60@pci:0000:c1:00.0
            # print(parts[-1])
            match = re.search(r'mlx5_(\w+\d+)@', parts[-1])
            queue = match.group(1) if match else 'unknown'

            # Read affinity from /proc/irq/<IRQ>/smp_affinity_list
            affinity = read_file(
                f"/proc/irq/{irq}/smp_affinity_list", "unknown")

            # Read effective_affinity from /proc/irq/<IRQ>/smp_affinity_list
            effective_affinity = read_file(
                f"/proc/irq/{irq}/effective_affinity_list", "unknown")

            irq_map[int(irq)] = {
                'queue': queue,
                'affinity': affinity,
                'effective_affinity': effective_affinity,
                'triggered': irq_count,
            }

    return irq_map


def print_irq_affinity_map(mapping, triggered_per_s=None):
    header = f"{'IRQ':<6} | {'NVMe Queue':<10} | {
        'Affinity (CPUs)':<15} | {'Effective (CPUs)':<16} | {'Triggered':<10}"
    if triggered_per_s is not None:
        header += " | Triggered/s"
    print(header)
    print("-" * (len(header)))

    for irq in sorted(mapping.keys()):
        info = mapping[irq]
        line = f"{irq:<6} | {info['queue']:<10} | {info['affinity']:<15} | {
            info['effective_affinity']:<16} | {info['triggered']:<10}"
        if triggered_per_s is not None:
            # line += f" | {triggered_per_s.get(irq, 0):<10.2f}"
            line += f" | {triggered_per_s.get(irq, 0):>10.0f}"
        print(line)


def compute_triggered_per_s(prev, curr, interval):
    rates = {}
    for irq in curr:
        if irq in prev:
            diff = curr[irq]['triggered'] - prev[irq]['triggered']
            rates[irq] = diff / interval
    return rates


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Show IRQ affinity and interrupt counts.")
    parser.add_argument("device", help="Device identifier, e.g., c1:00.0")
    parser.add_argument("--watch", type=float, nargs='?', const=1.0,
                        help="Watch mode, refresh every N seconds (default 1s)")
    args = parser.parse_args()

    if args.watch:
        prev_map = get_irq_affinity(args.device)
        while True:
            time.sleep(args.watch)
            curr_map = get_irq_affinity(args.device)
            rates = compute_triggered_per_s(prev_map, curr_map, args.watch)
            os.system("clear")
            print_irq_affinity_map(curr_map, rates)
            prev_map = curr_map
    else:
        irq_affinity_map = get_irq_affinity(args.device)
        if irq_affinity_map:
            print_irq_affinity_map(irq_affinity_map)
        else:
            print(f"No IRQs found for device {args.device}")
