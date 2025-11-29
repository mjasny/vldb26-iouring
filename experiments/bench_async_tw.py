from distexprunner import (
    Action, Server, ServerList,  ProcessGroup, IterClassGen, ParameterGrid,
    CSVGenerator, ComputedParam, Action,
    reg_exp, run_on_all, log, sleep
)
from utils import (
    StdoutBuffer, AttrDict, StatsAggr,
    set_kernel_version, set_mitigations,
    fmt_args, PerfOut
)


SERVER_PORT = 20000
PROJECT_DIR = '~/ringding/'
CLEAN_BUILD = False
DEBUG = False
BIN = 'bench_nop'


server_list = ServerList(
    Server('fn01', '10.0.21.51', port=SERVER_PORT, ib_ip='192.168.1.11'),
    Server('fn02', '10.0.21.52', port=SERVER_PORT, ib_ip='192.168.1.12'),
    # Server('fn04', '10.0.21.54', port=SERVER_PORT, ib_ip='192.168.1.14'),
    # Server('fn05', '10.0.21.55', port=SERVER_PORT, ib_ip='192.168.1.15'),
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


reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_kernel_version)
reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_mitigations)


@reg_exp(servers=server_list.unique_by_ip, run_always=True)
def prepare(servers):
    servers.cd(PROJECT_DIR)
    run_on_all(servers, './script/set_cpu_governor.sh')


@reg_exp(servers=server_list, run_always=True, raise_on_rc=False)
def pkill(servers):
    run_on_all(servers, f'sudo pkill -f ./build/{BIN}', verify_rc=False)


def bench_nop(servers, csv_file, run, **kwargs):
    servers.cd(PROJECT_DIR)

    csvs = IterClassGen(
        CSVGenerator,
        r'cycles=(?P<cycles>\d+)',
        r'secs=(?P<runtime>\d+(?:\.\d+)?)',
        r'ops_per_sec=(?P<ops_per_sec>.+$)',
        CSVGenerator.Array(r'latency=(?P<latency>\d+(?:\.\d+)?)'),
        run=run,
        **kwargs,
    )

    procs = ProcessGroup()
    for s in servers:
        args = AttrDict(
            **kwargs
        )
        csv = next(csvs)
        csv.add_columns(
            kernel=s.kernel,
            mitigations=s.mitigations,
            node=s.id,
        )
        cmd = f'./build/{BIN} {fmt_args(args)}'
        procs.add(s.run_cmd(cmd, stdout=[csv]))

    procs.wait()

    for csv in csvs:
        csv.write(csv_file)


def run(params):
    reg_exp(servers=server_list, params=params,
            raise_on_rc=False)(bench_nop)


RUNS = 5
params = ParameterGrid(
    csv_file='data/bench_async_tw.csv',
    run=range(RUNS),

    core_id=[3],
    setup_mode=['sqpoll', 'defer'],
    duration=[20_000],
    measure_lat=[True],
    nr_nops=[1],
    test_tw=[False, True],
    stats_interval=[100_000],

    pin_iowq=[True],
    max_workers=[8],
    sleep_us=[1],
)
run(params)
