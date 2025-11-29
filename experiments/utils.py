import numpy as np
import json
import os
import typing
import itertools
import csv
from distexprunner import ProcessGroup, IterClassGen, CSVGenerator

KiB = 1024
MiB = KiB * 1024


def fmt_args(args):
    from collections.abc import Iterable
    d = []
    for k, v in args.items():
        d.append(f'--{k}')
        if isinstance(v, Iterable) and not isinstance(v, str):
            d.append(','.join(map(str, v)))
        else:
            d.append(f'{v}')
    return ' '.join(d)


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


class StdoutBuffer:
    def __init__(self):
        self.buf = []

    def __call__(self, line):
        self.buf.append(line)

    def get(self, strip=True):
        if strip:
            return ''.join(self.buf).strip()
        return ''.join(self.buf)


def output_from_all(servers, cmd, verify_rc=True):
    bufs = IterClassGen(StdoutBuffer)
    procs = ProcessGroup()
    for s in servers:
        procs.add(s.run_cmd(cmd, stdout=next(bufs)))
    procs.wait(verify_rc=verify_rc)
    return list(b.get() for b in bufs)


class StatsAggr:
    def __init__(self):
        self._lines = []
        self._data = []
        self.parsed = False

    def __call__(self, line):
        if line.startswith("ts="):
            self._lines.append(line)

    def get_data(self):
        if self.parsed:
            return self._data

        for line in self._lines:
            line = line.strip()
            kvs = line.split(' ')
            entry = AttrDict()
            for kv in kvs:
                key, val = kv.split('=')
                try:
                    entry[key] = eval(val)
                except NameError:
                    entry[key] = val
            assert ('ts' in entry)
            self._data.append(entry)
        self.parsed = True
        return self._data

    def write(self, file, csv: None):
        if csv is not None:
            assert (isinstance(csv, CSVGenerator))

        stats_keys = list(dict.fromkeys(k for row in self.get_data()
                          for k in row.keys()))
        combined_header = list(csv.as_dict.keys()) + stats_keys

        write_header = not os.path.exists(file)
        if not write_header:
            header = next(open(file, 'r')).strip().split(',')
            if header != combined_header:
                print(header)
                print(combined_header)
            assert (header == combined_header)
        else:
            csv_header = ','.join(combined_header)
            # read header and check

        csv_values = csv.as_dict.values()
        with open(file, 'a+') as f:
            if write_header:
                f.write(f'{csv_header}\n')

            for row in self.get_data():
                data = itertools.chain(
                    csv_values, (row.get(x, '') for x in stats_keys))
                csv_row = ','.join(map(str, data))
                f.write(f'{csv_row}\n')


def calculate_statistics(numbers, percentage):
    if not 0 <= percentage < 50:
        raise ValueError("Percentage must be between 0 and less than 50.")

    # Sort the numbers
    numbers = sorted(numbers)

    # Calculate the number of elements to cut off
    n = int(len(numbers) * (percentage / 100))

    # Slice the list to exclude the first and last n%
    trimmed_numbers = numbers[n:len(numbers) - n]

    if len(trimmed_numbers) <= 1:
        raise ValueError("Not enough trimmed_numbers")

    return AttrDict(
        avg=np.mean(trimmed_numbers),
        min=np.min(trimmed_numbers),
        max=np.max(trimmed_numbers),
        std=np.std(trimmed_numbers),
        samples=len(trimmed_numbers),
    )


def prefix_keys(d, prefix):
    return {f'{prefix}{k}': v for k, v in d.items()}


class PerfOut:
    HEADER = [
        'cycles', 'kcycles', 'instructions', 'L1-misses', 'LLC-misses',
        'branch-misses', 'task-clock', 'scale', 'IPC', 'CPUs', 'GHz'
    ]

    def __init__(self):
        self._data = AttrDict()
        self._header = ''
        self._vals = ''
        self._next_data = False
        self.parsed = False

    def __call__(self, line):
        if line.lstrip().startswith('cycles,'):
            self._header = line.strip()
            self._next_data = True
        elif self._next_data:
            self._next_data = False
            self._vals = line.strip()

    def get_data(self):
        if self.parsed:
            return self._data

        if not self._header:
            self._data = AttrDict({f'perf_{x}': '' for x in self.HEADER})
            self.parsed = True
            return self._data

        cols = [x.strip() for x in self._header.split(',')]
        assert (cols == self.HEADER)
        header = list(f'perf_{x}' for x in cols)
        data = list(x.strip() for x in self._vals.split(','))
        self._data = AttrDict(zip(header, data))
        self.parsed = True
        return self._data


class PidStat:
    HEADER = ['ts', 'uid', 'pid', 'pct_usr', 'pct_system',
              'pct_guest', 'pct_wait', 'pct_cpu', 'cpu', 'cmd']

    def __init__(self):
        self.data = []
        self.to_skip = -1
        self.base_ts = 0

    def __call__(self, line):
        line = line.strip()
        if line.startswith('Linux'):
            self.to_skip = 2
            return
        if self.to_skip > 0:
            if self.to_skip == 1:
                self.base_ts = int(line.split()[0])
            self.to_skip -= 1
            return
        if not line:
            return

        values = line.split()
        assert (len(values) == len(self.HEADER))

        entry = AttrDict(zip(self.HEADER, map(self._parse, values)))
        entry.ts -= self.base_ts + 1
        self.data.append(entry)

    def _parse(self, value):
        try:
            if '.' in value:
                return float(value)
            return int(value)
        except ValueError:
            return value


class UringWorker:
    def __init__(self):
        self.lines = []
        self._workers = None

    def __call__(self, line):
        line = line.strip()
        if line:
            self.lines.append(line)

    @property
    def workers(self):
        if self._workers is not None:
            return self._workers

        self._workers = 0
        reader = csv.DictReader(self.lines)
        for row in reader:
            if 'iou-wrk' not in row['child_type']:
                continue
            self._workers = max(self._workers, int(row['child_count']))
        return self._workers


def set_kernel_version(servers):
    cmd = 'uname -r'
    bufs = IterClassGen(StdoutBuffer)
    procs = ProcessGroup()
    for s in servers:
        procs.add(s.run_cmd(cmd, stdout=next(bufs)))
    procs.wait()
    for i, buf in enumerate(bufs):
        servers[i].kernel = buf.get()


def set_mitigations(servers):
    cmd = 'cat /proc/cmdline'
    bufs = IterClassGen(StdoutBuffer)
    procs = ProcessGroup()
    for s in servers:
        procs.add(s.run_cmd(cmd, stdout=next(bufs)))
    procs.wait()
    for i, buf in enumerate(bufs):
        disabled = 'mitigations=off' in buf.get()
        servers[i].mitigations = not disabled


def set_mtu(servers, iface, value=None):
    if value is not None:
        cmd = f'sudo ip l set dev {iface} mtu {value}'
        procs = ProcessGroup()
        for s in servers:
            procs.add(s.run_cmd(cmd))
        procs.wait()

    cmd = f'cat /sys/class/net/{iface}/mtu'
    bufs = IterClassGen(StdoutBuffer)
    procs = ProcessGroup()
    for s in servers:
        procs.add(s.run_cmd(cmd, stdout=next(bufs)))
    procs.wait()
    for i, buf in enumerate(bufs):
        servers[i].mtu = buf.get()


def set_ssds(servers, name='KIOXIA'):
    cmd = 'sudo nvme list --output-format=json'
    bufs = IterClassGen(StdoutBuffer)
    procs = ProcessGroup()
    for s in servers:
        procs.add(s.run_cmd(cmd, stdout=next(bufs)))
    procs.wait()
    for i, buf in enumerate(bufs):
        data = json.loads(buf.get())
        ssds = []
        pt_ssds = []
        for dev in data['Devices']:
            if name not in dev['ModelNumber']:
                continue
            ssds.append(dev['DevicePath'])
            if 'GenericPath' in dev:
                pt_ssds.append(dev['GenericPath'])
            else:
                path = dev['DevicePath'].replace('/nvme', '/ng')
                pt_ssds.append(path)
        servers[i].ssds = ssds
        servers[i].pt_ssds = pt_ssds


class PerfParser:
    def __init__(self):
        self.header = None
        self.keys = None
        self.rows = []
        self._extra = {}

    def _normalize_key(self, seg):
        seg = seg.strip().lower()
        seg = seg.replace("%", "")
        seg = seg.replace("mb/s", "")
        seg = seg.replace("/", "")
        seg = seg.replace(" ", "")
        return seg

    def __call__(self, line):
        if line is None:
            return None
        line = line.strip().rstrip(',')
        if not line:
            return None

        # First line: header
        if self.header is None:
            # Remove leading # if present
            header = line.lstrip("#").strip()
            self.header = header
            parts = [self._normalize_key(p)
                     for p in header.split(",") if p.strip()]
            self.keys = parts
            return None

        # Repeated header line (exact match)
        if line == self.header:
            return None

        # Data line
        if not self.keys:
            return None

        vals = [v.strip() for v in line.split(",")]
        if len(vals) != len(self.keys):
            print(vals, '!=', self.keys)
            return None

        row = {k: v for k, v in zip(self.keys, vals)}
        self.rows.append(row)

    def add_columns(self, **kwargs):
        self._extra.update(kwargs)

    def write(self, file):
        write_header = not os.path.exists(file)
        header = list(self._extra.keys()) + self.keys
        with open(file, 'a+') as f:
            if write_header:
                line = ','.join(map(str, header))
                f.write(f'{line}\n')
            for row in self.rows:
                vals = list(self._extra.values()) + [row[x] for x in self.keys]
                line = ','.join(map(str, vals))
                f.write(f'{line}\n')


class CSVOutput:
    def __init__(self):
        self._header = None
        self._data = []
        self.parsed = False

    def __call__(self, line):
        line = line.strip()
        if self._header is None:
            self._header = line.split(',')
            return

        data = (x.strip() for x in line.split(','))
        self._data.append(dict(zip(self._header, data)))

    def write(self, file, csv: None):
        if csv is not None:
            assert (isinstance(csv, CSVGenerator))

        combined_header = list(csv.as_dict.keys()) + self._header

        write_header = not os.path.exists(file)
        if not write_header:
            header = next(open(file, 'r')).strip().split(',')
            if header != combined_header:
                print(header)
                print(combined_header)
            assert (header == combined_header)
        else:
            csv_header = ','.join(combined_header)
            # read header and check

        csv_values = csv.as_dict.values()
        with open(file, 'a+') as f:
            if write_header:
                f.write(f'{csv_header}\n')

            for row in self._data:
                data = itertools.chain(
                    csv_values, (row.get(x, '') for x in self._header))
                csv_row = ','.join(map(str, data))
                f.write(f'{csv_row}\n')
