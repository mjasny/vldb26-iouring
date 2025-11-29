from distexprunner import (
    Action, Server, ServerList,  ProcessGroup, IterClassGen, ParameterGrid,
    CSVGenerator, ComputedParam, Action,
    reg_exp, run_on_all, log, sleep
)

from utils import (
    StdoutBuffer, AttrDict, PerfOut,
    fmt_args, set_mitigations, set_kernel_version
)


SERVER_PORT = 20000
PROJECT_DIR = '~/ringding/'
CLEAN_BUILD = False
DEBUG = False
BIN = 'bench_buf_reg'


server_list = ServerList(
    # Server('fn01', '10.0.21.51', port=SERVER_PORT),
    Server('fn02', '10.0.21.52', port=SERVER_PORT),
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


def bench_buf_clone(servers, csv_file, run, **kwargs):
    servers.cd(PROJECT_DIR)

    csvs = IterClassGen(
        CSVGenerator,
        CSVGenerator.Mean(r'outer_duration=(?P<outer_duration>\d+)'),
        CSVGenerator.Mean(r'reg_duration=(?P<reg_duration>\d+)'),
        CSVGenerator.Mean(r'clone_duration=(?P<clone_duration>\d+)'),
        run=run,
    )

    if kwargs['use_hugepages']:
        n_pages = kwargs['mem_size'] // (2*MiB) + 10
    else:
        n_pages = 0
    cmd = f'echo {n_pages} | sudo tee ' + \
        '/sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages'
    run_on_all(servers, cmd)

    perfs = IterClassGen(PerfOut)

    procs = ProcessGroup()
    for s in servers:
        args = AttrDict(
            **kwargs
        )
        cmd = f'sudo ./build/{BIN} {fmt_args(args)}'
        csv = next(csvs)
        csv.add_columns(
            kernel=s.kernel,
            node=s.id,
            **args,
        )
        procs.add(s.run_cmd(cmd, stdout=[csv, next(perfs)]))

    procs.wait()

    for csv, perf in zip(csvs, perfs):
        csv.add_columns(**perf.get_data())
        csv.write(csv_file)


def run(params):
    reg_exp(servers=server_list, params=params,
            raise_on_rc=False)(bench_buf_clone)


KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB

RUNS = 5
params = ParameterGrid(
    csv_file='bench_buf_reg.csv',  # with chunk_size
    run=range(RUNS),
    core_id=[3],
    perfevent=[False],
    use_hugepages=[False, True],
    chunk_size=[1*GiB, 100*MiB, 500*MiB],
    mem_size=[32*GiB, 512*GiB][1:],
    mode=['clone'],
    num_threads=[64],
)
run(params)
