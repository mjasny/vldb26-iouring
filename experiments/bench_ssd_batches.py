from distexprunner import (
    Action, Server, ServerList,  ProcessGroup, IterClassGen, ParameterGrid,
    CSVGenerator, ComputedParam, Action, File,
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
BIN = 'bench_ssd_batches'


server_list = ServerList(
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


@reg_exp(servers=server_list, run_always=True, raise_on_rc=False)
def clear_csvs(servers):
    servers.cd(PROJECT_DIR)
    run_on_all(servers, 'rm -f ./latencies.csv')
    run_on_all(servers, 'rm -f ./outstanding_io.csv')


def bench_ssd_batches(servers, csv_file, run, **kwargs):
    servers.cd(PROJECT_DIR)

    csvs = IterClassGen(
        CSVGenerator,
        run=run,
        **kwargs,
    )

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
            ssds=s.ssds[1]
        )
        cmd = f'sudo ./build/{BIN} {fmt_args(args)}'
        procs.add(s.run_cmd(cmd, stdout=[csv]))

    procs.wait()

    procs = ProcessGroup()
    for s in servers:
        cmd = 'cat ./latencies.csv'
        procs.add(s.run_cmd(cmd, stdout=File(
            'data', 'bench_ssd_batches_latencies.csv', append=True)))
        cmd = 'cat ./outstanding_io.csv'
        procs.add(s.run_cmd(cmd, stdout=File(
            'data', 'bench_ssd_batches_outstanding.csv', append=True)))

    procs.wait()

    run_on_all(servers, "sed -i '2,$d' ./latencies.csv")
    run_on_all(servers, "sed -i '2,$d' ./outstanding_io.csv")

    for csv in csvs:
        csv.write(csv_file)


def run(params):
    reg_exp(servers=server_list, params=params,
            raise_on_rc=False)(bench_ssd_batches)


KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB

RUNS = 1
params = ParameterGrid(
    csv_file='data/bench_ssd_batches.csv',
    run=range(RUNS),

    iopoll=[False],
    duration=[10_000],
    num_threads=[8],
    ssd_size=[1*GiB],
    batch_size=[1, 8, 32, 64, 128, 256],
    label=ComputedParam(lambda batch_size: [f'Batchsize_{batch_size}']),
    spiky=[False],
    target_rate=[1_500_000],
)
run(params)
