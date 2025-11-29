from distexprunner import (
    Action, Server, ServerList,  ProcessGroup, IterClassGen, ParameterGrid,
    CSVGenerator, ComputedParam, Action,
    reg_exp, run_on_all, log, sleep
)
from utils import (
    StdoutBuffer, AttrDict, StatsAggr,
    set_kernel_version, set_mitigations, set_ssds,
    fmt_args, PerfOut
)


SERVER_PORT = 20000
PROJECT_DIR = '~/ringding/'
CLEAN_BUILD = False
DEBUG = False
BIN = 'buffer_mgr'


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


reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_kernel_version)
reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_mitigations)
reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_ssds)


@reg_exp(servers=server_list, run_always=True, raise_on_rc=False)
def pkill(servers):
    run_on_all(servers, f'sudo pkill -f ./build/{BIN}', verify_rc=False)


def bench_buffer_mgr(servers, csv_file, ssd_id, run, **kwargs):
    servers.cd(PROJECT_DIR)

    csvs = IterClassGen(
        CSVGenerator,
        run=run,
        **kwargs,
    )

    stats = IterClassGen(StatsAggr)

    procs = ProcessGroup()
    for s in servers:
        csv = next(csvs)
        ssd_path = s.ssds[ssd_id]
        if kwargs['nvme_cmds']:
            ssd_path = ssd_path.replace('/dev/nvme', '/dev/ng')
        csv.add_columns(
            kernel=s.kernel,
            mitigations=s.mitigations,
            node=s.id,
            ssd=ssd_path,
        )

        args = AttrDict(
            ssd=ssd_path,
            **kwargs,
        )

        cmd = f'sudo ./build/{BIN} {fmt_args(args)}'
        procs.add(s.run_cmd(cmd, stdout=[csv, next(stats)]))

    procs.wait()

    for csv, stat in zip(csvs, stats):
        stat.write(csv_file, csv=csv)


def run(params):
    reg_exp(servers=server_list, params=params,
            raise_on_rc=False)(bench_buffer_mgr)


KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB

RUNS = 1
params = ParameterGrid(
    csv_file='data/bench_buffer_mgr.csv',
    run=range(RUNS),
    ssd_id=[0],

    setup_mode=['defer'],
    workload=['ycsb'],
    submit_always=[False],
    sync_variant=[False],
    posix_variant=[False],

    duration=[10_000],
    virt_size=[1*GiB],
    free_target=[0.10],
    page_table_factor=[2.5],
    concurrency=[128],
    evict_batch=[128],

    reg_ring=[False],
    reg_fds=[False],
    reg_bufs=[False],
    nvme_cmds=[False],
    iopoll=[False],

    ycsb_read_ratio=[0],
    ycsb_tuple_count=[10_000_000],
    tpcc_warehouses=[1],

    libaio=[False],
)


# # in-memory baseline
# run(params.update(
#     virt_size=[4*GiB],
#     iopoll=[False],
# ))
#
# # synchronous, with and without batching
# run(params.update(
#     concurrency=[1],
#     evict_batch=[1, 128],
#     sync_variant=[True],
#     iopoll=[False, True],
# ))
#
# # posix
# run(params.update(
#     concurrency=[1],
#     evict_batch=[1, 128],
#     sync_variant=[True],
#     posix_variant=[True],
# ))
#
# # async, with and without submit batching
# run(params.update(
#     concurrency=[128],
#     evict_batch=[128],
#     submit_always=[False, True],
#     iopoll=[False, True],
# ))
#
# # with optimizations
# run(params.update(
#     concurrency=[128],
#     evict_batch=[128],
#     reg_ring=[True],
#     reg_fds=[True],
#     reg_bufs=[True],
#     nvme_cmds=[False, True],
#     iopoll=[False, True],
# ))
#
#
# # SQPoll
# run(params.update(
#     setup_mode=['sqpoll'],
#     concurrency=[128],
#     evict_batch=[128],
#     reg_ring=[True],
#     reg_fds=[True],
#     reg_bufs=[True],
#     nvme_cmds=[True],
#     iopoll=[False, True],
# ))
#
# # -------------------------

tpcc_concurrency = 128  # 64 #32
tpcc_duration = 200_000

# TPC-C basic and concurrent
run(params.update(
    workload=['tpcc'],
    concurrency=[1, tpcc_concurrency],
    evict_batch=[tpcc_concurrency],
    duration=[tpcc_duration],
    ycsb_read_ratio=[''],
    ycsb_tuple_count=[''],
    tpcc_warehouses=[1, 100],
    iopoll=[False, True],
))

# TPC-C optimized
run(params.update(
    workload=['tpcc'],
    concurrency=[tpcc_concurrency],
    evict_batch=[tpcc_concurrency],
    duration=[200_000],
    reg_ring=[True],
    reg_fds=[True],
    reg_bufs=[True],
    nvme_cmds=[False, True],
    ycsb_read_ratio=[''],
    ycsb_tuple_count=[''],
    tpcc_warehouses=[1, 100],
    iopoll=[False, True],
))

# SQPoll
run(params.update(
    workload=['tpcc'],
    setup_mode=['sqpoll'],
    concurrency=[tpcc_concurrency],
    evict_batch=[tpcc_concurrency],
    duration=[200_000],
    reg_ring=[True],
    reg_fds=[True],
    reg_bufs=[True],
    nvme_cmds=[True],
    ycsb_read_ratio=[''],
    ycsb_tuple_count=[''],
    tpcc_warehouses=[1, 100],
    iopoll=[False, True],
))

# -------------------------

# Enable libaio for kuring reactor

# async, with and without submit batching
# run(params.update(
#    concurrency=[128],
#    evict_batch=[128],
#    submit_always=[False, True],
#    libaio=[True],
# ))
# TPC-C concurrent
# run(params.update(
#    workload=['tpcc'],
#    concurrency=[tpcc_concurrency],
#    evict_batch=[tpcc_concurrency],
#    duration=[tpcc_duration],
#    ycsb_read_ratio=[''],
#    ycsb_tuple_count=[''],
#    tpcc_warehouses=[1, 100],
#    libaio=[True],
# ))
