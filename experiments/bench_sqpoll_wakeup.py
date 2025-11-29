from distexprunner import (
    Action, Server, ServerList,  ProcessGroup, IterClassGen, ParameterGrid,
    CSVGenerator, ComputedParam, Action,
    reg_exp, run_on_all, log, sleep
)
from utils import (
    StdoutBuffer, AttrDict, StatsAggr,
    set_kernel_version, set_mitigations, set_ssds,
    calculate_statistics, prefix_keys,
    fmt_args, PerfOut
)


SERVER_PORT = 20000
PROJECT_DIR = '~/ringding/'
CLEAN_BUILD = False
DEBUG = False
BIN = 'bench_sqpoll_wakeup'


server_list = ServerList(
    # Server('fn05', '10.0.21.55', port=SERVER_PORT, ib_ip='192.168.1.15'),
    # Server('fn06', '10.0.21.56', port=SERVER_PORT, ib_ip='192.168.1.16'),
    # Server('fn03', '10.0.21.53', port=SERVER_PORT, ib_ip='192.168.1.13'),
    # Server('fn04', '10.0.21.54', port=SERVER_PORT, ib_ip='192.168.1.14'),
    # Server('fn02', '10.0.21.52', port=SERVER_PORT, ib_ip='192.168.1.12'),
    Server('fn01', '10.0.21.51', port=SERVER_PORT, ib_ip='192.168.1.11'),
)


@reg_exp(servers=server_list.unique_by_ip, run_always=True)
def compile(servers):
    servers.cd(PROJECT_DIR)

    cmake_args = ''
    if DEBUG:
        cmake_args = '-DCMAKE_BUILD_TYPE=Debug'

    if CLEAN_BUILD:
        run_on_all(servers, 'rm -rf build/')

    run_on_all(servers, f'cmake -B build/ {cmake_args}')
    run_on_all(servers, f'make -C build/ -j {BIN}')


@reg_exp(servers=server_list.unique_by_ip, run_always=True)
def prepare(servers):
    servers.cd(PROJECT_DIR)
    run_on_all(servers, './script/set_cpu_governor.sh')


reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_kernel_version)
reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_mitigations)
reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_ssds)


@reg_exp(servers=server_list, run_always=True, raise_on_rc=False)
def pkill(servers):
    servers.cd(PROJECT_DIR)
    run_on_all(servers, f'sudo pkill -f ./build/{BIN}', verify_rc=False)


def bench_sqpoll_wakeup(servers, csv_file, run, group, **kwargs):
    servers.cd(PROJECT_DIR)

    csvs = IterClassGen(
        CSVGenerator,
        r'avg=(?P<avg>\d+(?:\.\d+)?)',
        r'min=(?P<min>\d+(?:\.\d+)?)',
        r'p50=(?P<p50>\d+(?:\.\d+)?)',
        r'p90=(?P<p90>\d+(?:\.\d+)?)',
        r'p95=(?P<p95>\d+(?:\.\d+)?)',
        r'p99=(?P<p99>\d+(?:\.\d+)?)',
        r'max=(?P<max>\d+(?:\.\d+)?)',
        CSVGenerator.Array(r'latency=(?P<latency>\d+(?:\.\d+)?)'),
        run=run,
        group=group,
        **kwargs,
    )

    ssd_id = kwargs.pop('ssd_id')

    procs = ProcessGroup()
    for s in servers:
        csv = next(csvs)
        csv.add_columns(
            kernel=s.kernel,
            mitigations=s.mitigations,
            node=s.id,
        )

        args = AttrDict(
            **kwargs,
        )
        if not args.do_nops:
            args.file = s.ssds[ssd_id]

        cmd = f'./build/{BIN} {fmt_args(args)}'
        if not args.do_nops:
            cmd = f'sudo {cmd}'
        procs.add(s.run_cmd(cmd, stdout=[csv]))

    procs.wait()

    for csv in csvs:
        csv.write(csv_file)


def run(params):
    reg_exp(servers=server_list, params=params,
            raise_on_rc=False)(bench_sqpoll_wakeup)


RUNS = 5
vals = [
    # idle, interval
    (50, 25),
    (100, 50),
    (200, 100),
    (300, 150),
]
for idle, interval in vals:
    group = f'{idle}_{interval}'
    # awake
    params = ParameterGrid(
        csv_file='data/bench_sqpoll_wakeup.csv',
        run=range(RUNS),
        group=group,

        ssd_id=[1],
        ops=[100],
        idle_ms=[idle],  # until sqpoll thread turns off
        interval_ms=[interval],  # between each I/O
        do_nops=[True, False],
    )
    run(params)

    # wakeup
    idle, interval = interval, idle
    params = ParameterGrid(
        csv_file='data/bench_sqpoll_wakeup.csv',
        run=range(RUNS),
        group=group,

        ssd_id=[1],
        ops=[100],
        idle_ms=[idle],  # until sqpoll thread turns off
        interval_ms=[interval],  # between each I/O
        do_nops=[True, False],
    )
    run(params)
