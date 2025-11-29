import subprocess
import glob
from typing import List, Dict, Tuple, Set, Optional
import sys
import argparse
import os
import re
import time
import itertools


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def read_file(path):
    with open(path) as f:
        return f.read().strip()


def parse_mask(mask):
    if mask is None:
        return None

    mask = mask.strip()
    chunks = mask.split(',')

    cpus = []
    bit_index = 0
    for chunk in reversed(chunks):
        chunk = chunk.strip()
        if not chunk:
            bit_index += 32
            continue

        try:
            bits = int(chunk, 16)
        except ValueError:
            bits = 0

        # Extract set bits
        for bit in range(32):
            if bits & (1 << bit):
                cpus.append(bit_index + bit)

        bit_index += 32

    return sorted(cpus)


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
                f"/proc/irq/{irq}/smp_affinity_list")

            # Read effective_affinity from /proc/irq/<IRQ>/smp_affinity_list
            effective_affinity = read_file(
                f"/proc/irq/{irq}/effective_affinity_list")

            affinity_hint = read_file(
                f"/proc/irq/{irq}/affinity_hint")
            affinity_hint = parse_mask(affinity_hint)

            irq_map[int(irq)] = AttrDict(
                queue=queue,
                affinity=affinity,
                effective_affinity=effective_affinity,
                triggered=irq_count,
                affinity_hint=affinity_hint,
            )

    return irq_map


def norm_pci_addr(addr: str) -> str:
    """Normalize PCI address (e.g. c1:00.0 → 0000:c1:00.0)."""
    addr = addr.strip().lower()
    if len(addr.split(":")) == 2:
        addr = "0000:" + addr

    if not os.path.exists(f"/sys/bus/pci/devices/{addr}"):
        # try to find a unique match by suffix
        matches = [p for p in glob.glob(
            "/sys/bus/pci/devices/*") if p.endswith(addr)]
        if not matches:
            sys.exit(f"PCI device not found for '{addr}'")
        if len(matches) > 1:
            sys.exit(f"Ambiguous PCI fragment '{addr}': {
                     ', '.join(os.path.basename(m) for m in matches)}")
        addr = os.path.basename(matches[0])
    return addr


def ifaces_for_pci(pci_addr: str) -> list[str]:
    """Return list of interfaces under /sys/bus/pci/devices/<pci>/net."""
    net_dir = f"/sys/bus/pci/devices/{pci_addr}/net"
    if not os.path.isdir(net_dir):
        return []
    return sorted(os.listdir(net_dir))


def bounce_iface(iface: str) -> None:
    """Bring interface down then up."""
    print(f"Bouncing {iface} ...")
    subprocess.run(["ip", "link", "set", "dev", iface, "down"], check=True)
    subprocess.run(["ip", "link", "set", "dev", iface, "up"], check=True)


PROC_IRQ_FMT = "/proc/irq/{irq}/smp_affinity_list"


def write_affinity(irq: int, cpus: List[int]) -> None:
    val = ",".join(str(c) for c in cpus)
    path = PROC_IRQ_FMT.format(irq=irq)
    with open(path, "w") as f:
        f.write(val+"\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Show IRQ affinity and interrupt counts.")
    parser.add_argument("device", help="Device identifier, e.g., c1:00.0")
    parser.add_argument("--watch", type=float, nargs='?', const=1.0,
                        help="Watch mode, refresh every N seconds (default 1s)")
    parser.add_argument('--from_hint', action='store_true',
                        help='Set affinity from hint')
    parser.add_argument('--dry_run', action='store_true',
                        help='Dry-run')
    args = parser.parse_args()

    irq_affinity_map = get_irq_affinity(args.device)

    NUM_CHIPLETS = 12
    CORES_PER_CHIPLET = 8
    idx = 0

    cpu_map = [
        # Chiplet 0
        0, 1, 2, 3, 4, 5,
        # Chiplet 1
        8, 9, 10, 11, 12, 13,
        # Chiplet 2
        16, 17, 18, 19, 20, 21,
        # Chiplet 3
        24, 25, 26, 27, 28, 29,
        # Chiplet 4
        32, 33, 34, 35, 36, 37,
        # Chiplet 5
        40, 41, 42, 43, 44, 45,
        # Chiplet 6
        48, 49, 50, 51, 52, 53,
        # Chiplet 7
        56, 57, 58, 59, 60, 61,
        # Chiplet 8
        64, 65, 66, 67,
        # Chiplet 9
        72, 73, 74, 75,
        # Chiplet 10
        80, 81, 82, 83,
        # Chiplet 11
        88, 89, 90, 91,
    ]
    print(len(cpu_map))
    assert (len(cpu_map) == 64)

    cpu_to_queue = []
    for irq, info in irq_affinity_map.items():
        if not info.queue.startswith('comp'):
            continue

        chiplet = idx % NUM_CHIPLETS
        core_in_chiplet = idx // NUM_CHIPLETS
        core_id = chiplet * CORES_PER_CHIPLET + core_in_chiplet
        # cpus = [core_id]
        cpus = [cpu_map[idx]]
        if args.from_hint:
            cpus = info.affinity_hint

        print('irq={} queue={} => cpu={}'.format(
            irq, info.queue, ','.join(map(str, cpus))))

        if not args.dry_run:
            write_affinity(irq, cpus)

        assert (len(cpus) == 1)
        cpu_to_queue.append((idx, cpus[0]))

        idx += 1

    pci = norm_pci_addr(args.device)
    ifaces = ifaces_for_pci(pci)

    if not ifaces:
        sys.exit(f"No network interfaces found under PCI {pci}")

    print(f"PCI {pci} -> interface(s): {', '.join(ifaces)}")

    if not args.dry_run:
        for iface in ifaces:
            bounce_iface(iface)
        print("All interface(s) bounced successfully.")

    it = itertools.chain(cpu_to_queue, cpu_to_queue)

    for i in range(32):
        tx, cpu = next(it)
        rx, _cpu = next(it)
        print(f'{{.core_id={cpu}, .tx_queue={tx}, .rx_queue={rx}}},')
        # {.core_id = 9, .tx_queue = 9, .rx_queue = 13},
