from distexprunner import (
    Action, Server, ServerList,  ProcessGroup, IterClassGen, ParameterGrid,
    CSVGenerator, ComputedParam, Action, SubstrMatcher,
    reg_exp, run_on_all, log, sleep
)
from utils import (
    StdoutBuffer, AttrDict, StatsAggr, PerfOut, PidStat,
    set_mitigations, set_kernel_version, set_mtu, set_ssds,
    fmt_args, calculate_statistics, prefix_keys, KiB, MiB, UringWorker
)


SERVER_PORT = 20000
PROJECT_DIR = '~/ringding/'
CLEAN_BUILD = False
DEBUG = False
RESET_SSD = True
BIN = 'bench_fsync'

server_list = ServerList(
    # Server('fn01', '10.0.21.51', port=SERVER_PORT),
    Server('c08', '10.0.21.18', port=SERVER_PORT),
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
# reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_ssds)
reg_exp(servers=server_list.unique_by_ip, run_always=True,
        params=ParameterGrid(name="Samsung SSD 980 PRO 1TB"))(set_ssds)


@reg_exp(servers=server_list, run_always=True, raise_on_rc=False)
def pkill(servers):
    run_on_all(servers, f'sudo pkill -f ./build/{BIN}', verify_rc=False)


def bench_fsync(servers, csv_file, run, ssd_id, **kwargs):
    servers.cd(PROJECT_DIR)

    csvs = IterClassGen(
        CSVGenerator,
        r'cycles=(?P<cycles>\d+)',
        r'secs=(?P<runtime>\d+(?:\.\d+)?)',
        r'ops_per_sec=(?P<ops_per_sec>.+$)',
        run=run,
    )

    uring_workers = IterClassGen(UringWorker)
    stats = IterClassGen(StatsAggr)

    if RESET_SSD:
        procs = ProcessGroup()
        for s in servers:
            ssd = s.ssds[ssd_id]
            cmd = f'./script/reset_ssds.sh {ssd}'
            procs.add(s.run_cmd(cmd, timeout=180))  # 3min

        if procs.has_timeout():
            return Action.RESTART

        procs.wait()
        sleep(1)

    procs = ProcessGroup()
    for s in servers:
        cmd = f'./script/io-worker.sh {BIN}'
        procs.add(s.run_cmd(cmd, stdout=[next(uring_workers)]))

        is_pt = 'nvme_passthru' in kwargs['method']
        ssds = s.pt_ssds if is_pt else s.ssds

        args = AttrDict(
            ssd=ssds[ssd_id],
            **kwargs,
        )

        csv = next(csvs)
        csv.add_columns(
            kernel=s.kernel,
            mitigations=s.mitigations,
            node=s.id,
            **args,
        )

        cmd = f'sudo ./build/{BIN} {fmt_args(args)}'
        procs.add(s.run_cmd(cmd, stdout=[csv, next(stats)]))

    procs.wait(verify_rc=True)

    for csv, info, stat in zip(csvs, uring_workers, stats):
        csv.add_columns(ioworker=info.workers)

        data = stat.get_data()

        latencies = [x.latency for x in data if 'latency' in x]
        lats = calculate_statistics(latencies, 10)
        csv.add_columns(**prefix_keys(lats, 'lat_'))

        ops = [x.ops for x in data if 'ops' in x]
        ops = calculate_statistics(ops, 10)
        csv.add_columns(**prefix_keys(ops, 'ops_'))

        csv.write(csv_file)
        log(f'Wrote: {csv_file}')
    sleep(1)


# Experiments


def run(params):
    reg_exp(servers=server_list, params=params,
            raise_on_rc=False)(bench_fsync)


params = ParameterGrid(
    csv_file='bench_fsync.csv',
    ssd_id=[0, 1, 2, 3],
    # ssd_id=[4, 5, 6, 7],

    run=range(3),
    core_id=[70],

    reg_ring=[True],
    reg_fds=[True],
    reg_bufs=[False],
    iopoll=[False, True],

    duration=[30_000],
    perfevent=[False],
    measure_lat=[True],
    # pin_iowq=[False, True][1:],
    pin_iowq=[False, True][:1],
    max_workers=[0],
    setup_mode=['defer', 'sqpoll'],
    # num_writes=[1, 2],
    write_size=[4*KiB],
    method=ComputedParam(lambda iopoll: [
        'none',
        # 'fsync',  # this is not durable
        'fsync_link',
        'fsync_link2',  # manually linked
        'open_sync',
        'open_dsync',
        'write_sync',
        'write_dsync',
        'nvme_passthru',
        'nvme_passthru_flush',
    ] if not iopoll else [
        'none',
        'open_sync',
        'open_dsync',
        'write_sync',
        'write_dsync',
        'fsync_link2',  # seq after ring
        'nvme_passthru',
        'nvme_passthru_flush',
    ]),
    # method=ComputedParam(lambda iopoll: [
    #    # 'none',
    #    'fsync',  # this is not durable
    #    'fsync_link',
    #    'fsync_link2',  # manually linked
    #    # 'open_sync',
    #    # 'open_dsync',
    #    # 'write_sync',
    #    # 'write_dsync',
    #    # 'nvme_passthru',
    #    # 'nvme_passthru_flush',
    # ]  # if not iopoll else [
    #    # 'none',
    #    # 'open_sync',
    #    # 'open_dsync',
    #    # 'write_sync',
    #    # 'write_dsync',
    #    # 'fsync_link2',  # seq after ring
    #    # 'nvme_passthru',
    #    # 'nvme_passthru_flush',
    #    # ]
    # ),
    # method=['nvme_passthru_flush'],  # manually linked
)
# run(params)


# c08 test
params = ParameterGrid(
    csv_file='bench_fsync.csv',
    ssd_id=[0, 1, 2, 3],

    run=range(3),
    core_id=[32],

    reg_ring=[True],
    reg_fds=[True],
    reg_bufs=[False],
    iopoll=[False, True],

    duration=[30_000],
    perfevent=[False],
    measure_lat=[True],
    pin_iowq=[False, True][:1],
    max_workers=[0],
    setup_mode=['defer', 'sqpoll'],
    # num_writes=[1, 2],
    write_size=[4*KiB],
    method=ComputedParam(lambda iopoll: [
        'none',
        # 'fsync',  # this is not durable
        'fsync_link',
        'fsync_link2',  # manually linked
        'open_sync',
        'open_dsync',
        'write_sync',
        'write_dsync',
        'nvme_passthru',
        'nvme_passthru_flush',
    ] if not iopoll else [
        'none',
        'open_sync',
        'open_dsync',
        'write_sync',
        'write_dsync',
        'fsync_link2',  # seq after ring
        'nvme_passthru',
        'nvme_passthru_flush',
    ]),
)
# run(params)


params = ParameterGrid(
    csv_file='bench_fsync.csv',
    ssd_id=[0, 1, 2, 3],

    run=range(3),
    core_id=[32],

    reg_ring=[True],
    reg_fds=[True],
    reg_bufs=[False],
    iopoll=[False],

    duration=[30_000],
    perfevent=[False],
    measure_lat=[True],
    pin_iowq=[True],
    max_workers=[0, 8],
    setup_mode=['defer', 'sqpoll'],
    # num_writes=[1, 2],
    write_size=[4*KiB],
    method=[
        'fsync_link',
        'fsync_link2',  # manually linked
    ],
)
run(params)
