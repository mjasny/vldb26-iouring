import sys
import os
import re


def read_file(path, default=None):
    try:
        with open(path) as f:
            return f.read().strip()
    except FileNotFoundError:
        if default is not None:
            return default
        raise


def get_nvme_irq_affinity(nvme_name):
    irq_map = {}

    with open('/proc/interrupts') as f:
        # header
        line = next(f)
        parts = line.strip().split()
        n_cpus = len(parts)

        for line in f:
            if nvme_name in line:
                parts = line.strip().split()
                irqs = list(map(int, parts[1:n_cpus+1]))

                # 1 CPU handled for this irq
                assert (sum(1 for x in irqs if x != 0) <= 1)
                irq_count = next((x for x in irqs if x != 0), -1)

                irq = parts[0].rstrip(':')
                # Extract queue name like nvme8q7
                match = re.search(r'(nvme\d+q\d+)', line)
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


def print_irq_affinity_map(mapping):
    print(f"{'IRQ':<6} | {'NVMe Queue':<10} | {
          'Affinity (CPUs)'} | {'Effective (CPUs)'} | {'Triggered'}")
    print("-" * 80)
    for irq in sorted(mapping.keys()):
        info = mapping[irq]
        print(f"{irq:<6} | {info['queue']:<10} | {
            info['affinity']:<15} | {info['effective_affinity']:<16} | {info['triggered']}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} /dev/nvme7n1")
        sys.exit(1)

    nvme_device = sys.argv[1]

    nvme_base = re.match(r'(?:\/dev\/)?(nvme\d+)', nvme_device)
    if not nvme_base:
        raise Exception(f"Invalid NVMe device: {nvme_device}")
    nvme_name = nvme_base.group(1)

    nvme_name_full = re.sub(r'(nvme)(\d+)', r'\1\2n1', nvme_name)
    if not os.path.exists(f'/sys/class/nvme/{nvme_name}/{nvme_name_full}'):
        nvme_name_full = re.sub(r'(nvme)(\d+)', r'\1\2c\2n1', nvme_name)

    # 96 cores for 64/128 queues, first 32 double
    n_queues = read_file(f'/sys/class/nvme/{nvme_name}/queue_count')
    poll_queues = read_file('/sys/module/nvme/parameters/poll_queues')
    print(f'Queues: {n_queues}    Poll-Queues: {poll_queues}')

    nr_requests = read_file(
        f'/sys/class/nvme/{nvme_name}/{nvme_name_full}/queue/nr_requests')
    max_hw_sectors = read_file(
        f'/sys/class/nvme/{nvme_name}/{nvme_name_full}/queue/max_hw_sectors_kb')
    print(f'nr_requests: {nr_requests} max_hw_sectors: {max_hw_sectors}KiB')

    irq_affinity_map = get_nvme_irq_affinity(nvme_name)
    if irq_affinity_map:
        print_irq_affinity_map(irq_affinity_map)
    else:
        print(f"No NVMe IRQs found for device {nvme_device}")


# root@fn02:/home/mjasny/ringding/script/linux# echo 4 > /sys/module/nvme/parameters/poll_queues
# root@fn02:/home/mjasny/ringding/script/linux# echo 1 > /sys/class/nvme/nvme2/reset_controller

# with poll_queues
# perf switches between 550K and 850K IOPS on cores that are ranges 0-1 or 2-3
# mjasny@fn01:~/ringding/fio$ python ../script/nvme_irq.py
# IRQ    | NVMe Queue | Affinity (CPU Core List)
# --------------------------------------------------
# 256    | nvme8q0    | 0-95
# 680    | nvme8q1    | 0-1
# 681    | nvme8q2    | 2-3
# 682    | nvme8q3    | 4-5
# 683    | nvme8q4    | 6-7
# 684    | nvme8q5    | 8-9
# 685    | nvme8q6    | 10-11
# 686    | nvme8q7    | 12-13
# 687    | nvme8q8    | 14-15
# 688    | nvme8q9    | 16-17
# 689    | nvme8q10   | 18-19
# 690    | nvme8q11   | 20-21
# 691    | nvme8q12   | 22-23
# 692    | nvme8q13   | 24-25
# 693    | nvme8q14   | 26-27
# 694    | nvme8q15   | 28-29
# 695    | nvme8q16   | 30-31
# 696    | nvme8q17   | 32-33
# 697    | nvme8q18   | 34-35
# 698    | nvme8q19   | 36-37
# 699    | nvme8q20   | 38-39
# 700    | nvme8q21   | 40-41
# 701    | nvme8q22   | 42-43
# 702    | nvme8q23   | 44-45
# 703    | nvme8q24   | 46-47
# 704    | nvme8q25   | 48-49
# 705    | nvme8q26   | 50-51
# 706    | nvme8q27   | 52-53
# 707    | nvme8q28   | 54-55
# 708    | nvme8q29   | 56-57
# 709    | nvme8q30   | 58-59
# 710    | nvme8q31   | 60-61
# 711    | nvme8q32   | 62-63
# 712    | nvme8q33   | 64
# 713    | nvme8q34   | 65
# 714    | nvme8q35   | 66
# 715    | nvme8q36   | 67
# 716    | nvme8q37   | 68
# 717    | nvme8q38   | 69
# 718    | nvme8q39   | 70
# 719    | nvme8q40   | 71
# 720    | nvme8q41   | 72
# 721    | nvme8q42   | 73
# 722    | nvme8q43   | 74
# 723    | nvme8q44   | 75
# 724    | nvme8q45   | 76
# 725    | nvme8q46   | 77
# 726    | nvme8q47   | 78
# 727    | nvme8q48   | 79
# 849    | nvme8q49   | 80
# 850    | nvme8q50   | 81
# 851    | nvme8q51   | 82
# 852    | nvme8q52   | 83
# 853    | nvme8q53   | 84
# 854    | nvme8q54   | 85
# 855    | nvme8q55   | 86
# 856    | nvme8q56   | 87
# 857    | nvme8q57   | 88
# 858    | nvme8q58   | 89
# 859    | nvme8q59   | 90
# 860    | nvme8q60   | 91
# 861    | nvme8q61   | 92
# 862    | nvme8q62   | 93
# 863    | nvme8q63   | 94
# 864    | nvme8q64   | 95


# without poll queues
# perf consistent 550K IOPS

# mjasny@fn01:~/ringding/fio$ python ../script/nvme_irq.py
# IRQ    | NVMe Queue | Affinity (CPU Core List)
# --------------------------------------------------
# 104    | nvme8q0    | 0-95
# 875    | nvme8q1    | 0
# 876    | nvme8q2    | 1
# 877    | nvme8q3    | 2
# 878    | nvme8q4    | 3
# 879    | nvme8q5    | 4
# 880    | nvme8q6    | 5
# 881    | nvme8q7    | 6
# 882    | nvme8q8    | 7
# 883    | nvme8q9    | 8
# 884    | nvme8q10   | 9
# 885    | nvme8q11   | 10
# 886    | nvme8q12   | 11
# 887    | nvme8q13   | 12
# 888    | nvme8q14   | 13
# 889    | nvme8q15   | 14
# 890    | nvme8q16   | 15
# 891    | nvme8q17   | 16
# 892    | nvme8q18   | 17
# 893    | nvme8q19   | 18
# 894    | nvme8q20   | 19
# 895    | nvme8q21   | 20
# 896    | nvme8q22   | 21
# 897    | nvme8q23   | 22
# 898    | nvme8q24   | 23
# 899    | nvme8q25   | 24
# 900    | nvme8q26   | 25
# 901    | nvme8q27   | 26
# 902    | nvme8q28   | 27
# 903    | nvme8q29   | 28
# 904    | nvme8q30   | 29
# 905    | nvme8q31   | 30
# 906    | nvme8q32   | 31
# 907    | nvme8q33   | 32
# 908    | nvme8q34   | 33
# 909    | nvme8q35   | 34
# 910    | nvme8q36   | 35
# 911    | nvme8q37   | 36
# 912    | nvme8q38   | 37
# 913    | nvme8q39   | 38
# 914    | nvme8q40   | 39
# 915    | nvme8q41   | 40
# 916    | nvme8q42   | 41
# 917    | nvme8q43   | 42
# 918    | nvme8q44   | 43
# 919    | nvme8q45   | 44
# 920    | nvme8q46   | 45
# 921    | nvme8q47   | 46
# 922    | nvme8q48   | 47
# 923    | nvme8q49   | 48
# ...
# 957    | nvme8q83   | 82
# 958    | nvme8q84   | 83
# 959    | nvme8q85   | 84
# 960    | nvme8q86   | 85
# 961    | nvme8q87   | 86
# 962    | nvme8q88   | 87
# 963    | nvme8q89   | 88
# 964    | nvme8q90   | 89
# 965    | nvme8q91   | 90
# 966    | nvme8q92   | 91
# 967    | nvme8q93   | 92
# 968    | nvme8q94   | 93
# 969    | nvme8q95   | 94
# 970    | nvme8q96   | 95
