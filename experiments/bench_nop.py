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


@reg_exp(servers=server_list, run_always=True, raise_on_rc=False)
def pkill(servers):
    run_on_all(servers, f'sudo pkill -f ./build/{BIN}', verify_rc=False)


def bench_nop(servers, csv_file, run, mode, **kwargs):
    servers.cd(PROJECT_DIR)

    kwargs = AttrDict(kwargs)
    kwargs.test_file = False
    kwargs.reg_fds = False
    kwargs.test_buf = False
    kwargs.reg_bufs = False

    match mode:
        case 'baseline':
            pass
        case 'buffer':
            kwargs.reg_bufs = True
        case 'file':
            kwargs.test_file = True
            pass
        case 'fixedfile':
            kwargs.test_file = True
            kwargs.reg_fds = True

    csvs = IterClassGen(
        CSVGenerator,
        r'cycles=(?P<cycles>\d+)',
        r'secs=(?P<runtime>\d+(?:\.\d+)?)',
        r'ops_per_sec=(?P<ops_per_sec>.+$)',
        run=run,
        mode=mode,
    )
    stats = IterClassGen(StatsAggr)

    perfs = IterClassGen(PerfOut)

    procs = ProcessGroup()
    for s in servers:
        args = AttrDict(
            core_id=8,
            **kwargs
        )
        csv = next(csvs)
        csv.add_columns(
            kernel=s.kernel,
            mitigations=s.mitigations,
            node=s.id,
            **args,
        )
        cmd = f'sudo ./build/{BIN} {fmt_args(args)}'
        procs.add(s.run_cmd(cmd, stdout=[csv, next(stats), next(perfs)]))

    procs.wait()

    for csv, stats in zip(csvs, stats):
        # data = stats.get_data()
        # latencies = [x.latency for x in data if 'latency' in x]
        # stats = calculate_statistics(latencies, 10)
        # csv.add_columns(**stats)
        pass

    for csv, perf in zip(csvs, perfs):
        csv.add_columns(**perf.get_data())
        csv.write(csv_file)


def run(params):
    reg_exp(servers=server_list, params=params,
            raise_on_rc=False)(bench_nop)


RUNS = 5
params = ParameterGrid(
    csv_file='data/bench_nop.csv',
    run=range(RUNS),

    perfevent=[False],
    reg_ring=[False, True],
    mode=['baseline', 'buffer', 'file', 'fixedfile'],

    setup_mode=['default', 'sqpoll', 'defer', 'coop'],
    duration=[30_000],
    nr_nops=[1, 8, 64, 512, 4096],
)
# run(params)


params = ParameterGrid(
    csv_file='data/bench_nop2.csv',
    run=range(RUNS),

    perfevent=[False, True],
    reg_ring=[False, True],
    mode=['baseline'],

    setup_mode=['defer'],
    duration=[30_000],
    nr_nops=[1],
)
# run(params)


params = ParameterGrid(
    csv_file='data/bench_nop3.csv',
    run=range(RUNS),

    perfevent=[True],
    reg_ring=[True],
    mode=['baseline'],

    setup_mode=['defer'],
    duration=[10_000],
    nr_nops=[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096],
)
run(params)
