from distexprunner import (
    Action,
    Server,
    ServerList,
    ProcessGroup,
    IterClassGen,
    ParameterGrid,
    CSVGenerator,
    ComputedParam,
    Action,
    reg_exp,
    run_on_all,
    log,
    sleep,
)
from utils import (
    StdoutBuffer,
    AttrDict,
    StatsAggr,
    set_kernel_version,
    set_mitigations,
    set_ssds,
    fmt_args,
    PerfOut,
    KiB,
    MiB,
    GiB,
)


SERVER_PORT = 20000
PROJECT_DIR = "~/ringding/"
CLEAN_BUILD = False
DEBUG = False
BIN = "buffer_mgr_mt"


server_list = ServerList(
    Server("fn01", "10.0.21.51", port=SERVER_PORT, ib_ip="192.168.1.11"),
)


@reg_exp(servers=server_list.unique_by_ip, run_always=True)
def compile(servers):
    servers.cd(PROJECT_DIR)

    cmake_args = ""
    if DEBUG:
        cmake_args = "-DCMAKE_BUILD_TYPE=Debug"

    if CLEAN_BUILD:
        run_on_all(servers, "rm -rf build/")

    run_on_all(servers, f"cmake -B build/ {cmake_args}")
    run_on_all(servers, f"make -C build/ -j {BIN}")


reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_kernel_version)
reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_mitigations)
reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_ssds)


@reg_exp(servers=server_list, run_always=True, raise_on_rc=False)
def pkill(servers):
    run_on_all(servers, f"sudo pkill -f ./build/{BIN}", verify_rc=False)


def bench_buffer_mgr(servers, csv_file, num_ssds, run, **kwargs):
    servers.cd(PROJECT_DIR)

    csvs = IterClassGen(
        CSVGenerator,
        run=run,
        **kwargs,
    )

    stats = IterClassGen(StatsAggr)

    args = AttrDict(
        **kwargs,
    )

    if (
        args.num_workers >= 64
        and args.iopoll
        and args.nvme_cmds
        and args.setup_mode == "defer"
    ):
        log("Skipping due to kernel panic")
        return

    procs = ProcessGroup()
    for s in servers:
        csv = next(csvs)
        ssd_paths = s.ssds[:num_ssds]
        if kwargs["nvme_cmds"]:
            ssd_paths = [x.replace("/dev/nvme", "/dev/ng") for x in ssd_paths]
        csv.add_columns(
            kernel=s.kernel,
            mitigations=s.mitigations,
            node=s.id,
            num_ssds=num_ssds,
        )

        args.ssds = ssd_paths

        cmd = f"sudo ./build/{BIN} {fmt_args(args)}"
        procs.add(s.run_cmd(cmd, stdout=[csv, next(stats)]))

    procs.wait()

    for csv, stat in zip(csvs, stats):
        stat.write(csv_file, csv=csv)


def run(params):
    reg_exp(servers=server_list, params=params, raise_on_rc=False)(bench_buffer_mgr)


RUNS = 1
params = ParameterGrid(
    csv_file="data/bench_buffer_mgr_mt.csv",
    run=range(RUNS),
    setup_mode=["defer"],
    workload=["ycsb"],
    submit_always=[False],
    sync_variant=[False],
    posix_variant=[False],
    duration=[10_000],
    virt_size=[1 * GiB],
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
    num_ssds=[1],
    num_workers=[1],
    # ycsb_zipf_theta=[0.0],
    ycsb_zipf_theta=[0.0, 0.5, 0.8, 0.9, 0.99],
)


# in-memory baseline
run(
    params.update(
        virt_size=[4 * GiB],
        iopoll=[False],
        ycsb_read_ratio=[0, 50, 100],
        num_workers=[1, 2, 4, 8, 16, 32, 64],
    )
)


params = params.update(
    concurrency=[128],
    evict_batch=[128],
    reg_ring=[False],
    reg_fds=[False],
    reg_bufs=[False],
    nvme_cmds=[False],
    iopoll=[False],
    num_ssds=[8],
    ycsb_read_ratio=[0],
    num_workers=[1, 8, 16, 32, 64],
    virt_size=[1 * GiB],
)
run(params)

run(
    params.update(
        submit_always=[True],
    )
)


params = params.update(
    reg_ring=[True],
    reg_fds=[True],
    reg_bufs=[True],
)
run(params)

run(
    params.update(
        nvme_cmds=[True],
        iopoll=ComputedParam(lambda nvme_cmds: [False, True] if nvme_cmds else [False]),
    )
)


# 10M  =>  2.97 GiB
# 20M  =>  5.94 GiB
# 30M  =>  8.91 GiB
# 40M  => 11.82 GiB
# 50M  => 14.81 GiB
# 100M => 27.57 GiB


# tpcc_concurrency = 128  # 64 #32
# tpcc_duration = 200_000
#
# params = params.update(
#    run=range(RUNS),
#    setup_mode=["defer"],
#    workload=["tpcc"],
#    concurrency=[tpcc_concurrency],
#    evict_batch=[tpcc_concurrency],
#    duration=[200_000],
#    submit_always=[False],
#    sync_variant=[False],
#    posix_variant=[False],
#    virt_size=[1 * GiB],
#    free_target=[0.10],
#    page_table_factor=[2.5],
#    reg_ring=[False],
#    reg_fds=[False],
#    reg_bufs=[False],
#    nvme_cmds=[False],
#    iopoll=[False],
#    ycsb_zipf_theta=[""],
#    ycsb_read_ratio=[""],
#    ycsb_tuple_count=[""],
#    tpcc_warehouses=[1],
#    libaio=[False],
#    num_ssds=[1],
#    num_workers=[1, 2, 4, 8, 16, 32, 64][::-1],
# )
#
## TPC-C basic and concurrent
# run(
#    params.update(
#        tpcc_warehouses=[1, 100],
#        iopoll=[False],
#    )
# )
#
## TPC-C optimized
# run(
#    params.update(
#        reg_ring=[True],
#        reg_fds=[True],
#        reg_bufs=[True],
#        nvme_cmds=[False, True],
#        tpcc_warehouses=[1, 100],
#        iopoll=[False, True],
#    )
# )
