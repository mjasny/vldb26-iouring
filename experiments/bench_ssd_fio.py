from distexprunner import (
    Action, Server, ServerList,  ProcessGroup, IterClassGen, ParameterGrid,
    CSVGenerator, ComputedParam, Action, File,
    reg_exp, run_on_all, log, sleep
)
from utils import (
    StdoutBuffer, AttrDict, StatsAggr,
    set_kernel_version, set_mitigations, set_ssds,
    calculate_statistics, prefix_keys,
    fmt_args, PerfOut, UringWorker
)


SERVER_PORT = 20000
PROJECT_DIR = '~/ringding/fio'
CLEAN_BUILD = False
DEBUG = False
BIN = 't/io_uring'


server_list = ServerList(
    Server('fn01', '10.0.21.51', port=SERVER_PORT, ib_ip='192.168.1.11'),
)


@reg_exp(servers=server_list.unique_by_ip, run_always=True)
def compile(servers):
    servers.cd(PROJECT_DIR)

    if CLEAN_BUILD:
        run_on_all(servers, f'make clean')
    run_on_all(servers, f'make -j')


@reg_exp(servers=server_list.unique_by_ip, run_always=True)
def prepare(servers):
    servers.cd(PROJECT_DIR)
    run_on_all(servers, '../script/set_cpu_governor.sh')


reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_kernel_version)
reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_mitigations)
reg_exp(servers=server_list.unique_by_ip, run_always=True,
        # params=ParameterGrid(name="Amazon EC2 NVMe Instance Storage"))(set_ssds)
        params=ParameterGrid(name="KIOXIA"))(set_ssds)


@reg_exp(servers=server_list, run_always=True, raise_on_rc=False)
def pkill(servers):
    servers.cd(PROJECT_DIR)
    run_on_all(servers, f'sudo pkill -f ./{BIN}', verify_rc=False)


def bench_ssd_fio(servers, csv_file, run, **kwargs):
    servers.cd(PROJECT_DIR)

    csvs = IterClassGen(
        CSVGenerator,
        CSVGenerator.Percentile(r'iops_tp=(?P<iops>\d+),', 0.50),
        CSVGenerator.Optional(r'cycles=(?P<cycles>\d+(?:\.\d+)?)'),
        CSVGenerator.Optional(r'instructions=(?P<instructions>\d+(?:\.\d+)?)'),
        CSVGenerator.Optional(r'task-clock=(?P<taskclk>\d+(?:\.\d+)?)'),
        CSVGenerator.Optional(r'l1-misses=(?P<l1_misses>\d+(?:\.\d+)?)'),
        CSVGenerator.Optional(r'llc-misses=(?P<llc_misses>\d+(?:\.\d+)?)'),
        CSVGenerator.Optional(
            r'branch-misses=(?P<branch_misses>\d+(?:\.\d+)?)'),

        CSVGenerator.Optional(r'scale=(?P<scale>\d+(?:\.\d+)?)'),
        CSVGenerator.Optional(r'IPC=(?P<ipc>\d+(?:\.\d+)?)'),
        CSVGenerator.Optional(r'CPUs=(?P<cpus>\d+(?:\.\d+)?)'),
        CSVGenerator.Optional(r'GHz=(?P<ghz>\d+(?:\.\d+)?)'),
        run=run,
        **kwargs,
    )

    args = AttrDict(kwargs)

    if args.nvme_cmds and args.bs > 512*KiB:
        log('Skipping too large BS for nvme_cmds')
        return

    if args.num_threads >= 32 and args.iopoll and args.nvme_cmds and args.bs == 4*KiB and args.mode == 'defer':
        log('Skipping due to kernel panic')
        return

    if args.posix:
        assert (not args.libaio)
        mode_arg = '-S 1'
    elif args.libaio:
        assert (not args.posix)
        mode_arg = '-a 1'
    else:
        mode_arg = '-S 0 -a 0 '
        if args.mode == 'defer':
            mode_arg += '-y 2'
        elif args.mode == 'coop':
            mode_arg += '-y 1'
        elif args.mode == 'sqpoll':
            mode_arg += '-y 3'
        elif args.mode == 'default':
            mode_arg += '-y 0'
        else:
            raise Exception("invalid mode")

    uring_workers = IterClassGen(UringWorker)

    procs = ProcessGroup()
    for s in servers:
        for tid in range(args.num_threads):
            cmd = '../script/io-worker.sh io_uring'
            procs.add(s.run_cmd(cmd, stdout=[next(uring_workers)]))

            csv = next(csvs)
            csv.add_columns(
                kernel=s.kernel,
                mitigations=s.mitigations,
                node=s.id,
                tid=tid,
            )

            flags = []
            flags.append(f'-r {args.duration}')
            flags.append(f'-d {args.iodepth}')
            flags.append(f'-b {args.bs}')
            flags.append(f'-s {args.batch_submit}')
            flags.append(f'-c {args.batch_complete}')
            flags.append(mode_arg)
            flags.append(f'-e {int(args.perfevent)}')
            flags.append(f'-X {int(args.reg_ring)}')
            flags.append(f'-F {int(args.reg_fds)}')
            flags.append(f'-B {int(args.reg_bufs)}')
            flags.append(f'-p {int(args.iopoll)}')
            flags.append(f'-u {int(args.nvme_cmds)}')
            flags.append(f'-O {int(not args.nvme_cmds)}')
            flags.append(f'-m {args.ssd_size // GiB}')
            flags.append(f'-w {int(args.write)}')

            ssds = s.pt_ssds if args.nvme_cmds else s.ssds
            ssds = ssds[:args.num_ssds]
            assert (len(ssds) == args.num_ssds)
            flags.append(' '.join(ssds))

            core = args.core_id + tid
            if 'fn' in s.id:
                core %= 96
            taskset = f'taskset -c {core}'
            if args.mode == 'sqpoll':
                taskset = ''
            opts = ' '.join(flags)
            cmd = f'sudo {taskset} ./{BIN} {opts}'
            # log(cmd)
            procs.add(s.run_cmd(cmd, stdout=[csv]))

    procs.wait()

    for csv, info in zip(csvs, uring_workers):
        csv.add_columns(ioworker=info.workers)
        csv.write(csv_file)


def run(params):
    reg_exp(servers=server_list, params=params,
            raise_on_rc=False)(bench_ssd_fio)


KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB
TiB = 1024 * GiB

# params = ParameterGrid(
#    csv_file='data/bench_ssd_bs_batches.csv',
#    run=range(1),
#
#    mode=['defer'],
#    posix=[False],
#    libaio=[False],
#    reg_ring=[False],
#    reg_fds=[False],
#    reg_bufs=[False],
#    nvme_cmds=[False],
#    iopoll=[False],
#
#    num_threads=[1],
#    core_id=[64],
#    num_ssds=[8],
#    perfevent=[True],
#    write=[False, True], # TODO rerun for write=True
#    duration=[10],  # in seconds
#    ssd_size=[1*TiB],
#    iodepth=[1, 2, 4, 8, 16, 32, 64, 128, 256, 512],
#    bs=[128*KiB, 256*KiB, 512*KiB],
#    batch_submit=ComputedParam(lambda iodepth: [iodepth]),
#    batch_complete=ComputedParam(lambda iodepth: [iodepth]),
# )
#
# basic
# run(params)
# run(params.update(
#    reg_ring=[True],
#    reg_fds=[True],
# ))
# run(params.update(
#    reg_ring=[True],
#    reg_fds=[True],
#    reg_bufs=[True],
# ))
# run(params.update(
#    reg_ring=[True],
#    reg_fds=[True],
#    reg_bufs=[True],
#    iopoll=[True],
# ))
# run(params.update(
#    reg_ring=[True],
#    reg_fds=[True],
#    reg_bufs=[True],
#    iopoll=[False, True],
#    nvme_cmds=[True],
# ))


# params = ParameterGrid(
#    csv_file='data/bench_ssd_flags.csv',
#    run=range(3),
#
#    mode=['defer', 'sqpoll'],
#    posix=[False],
#    libaio=[False],
#    reg_ring=[False],
#    reg_fds=[False],
#    reg_bufs=[False],
#    nvme_cmds=[False],
#    iopoll=[False],
#
#    num_threads=[1],
#    core_id=[64],
#    num_ssds=[8],
#    perfevent=[True, False],
#    write=[False],
#    duration=[10],  # in seconds
#    ssd_size=[1*TiB],
#    iodepth=[512],
#    bs=[4*KiB],
#    batch_submit=[32],
#    batch_complete=[32],
# )
# run(params)
# run(params.update(
#    reg_ring=[True],
# ))
# run(params.update(
#    reg_ring=[True],
#    reg_fds=[True],
# ))
# run(params.update(
#    reg_ring=[True],
#    reg_fds=[True],
#    reg_bufs=[True],
# ))
# run(params.update(
#    reg_ring=[True],
#    reg_fds=[True],
#    reg_bufs=[True],
#    nvme_cmds=[True],
# ))
# run(params.update(
#    reg_ring=[True],
#    reg_fds=[True],
#    reg_bufs=[True],
#    nvme_cmds=[True],
#    iopoll=[True],
# ))


# params = ParameterGrid(
#    csv_file='data/bench_ssd_bs.csv',
#    run=range(1),
#
#    mode=['defer'],
#    posix=[False],
#    libaio=[False],
#    reg_ring=[False],
#    reg_fds=[False],
#    reg_bufs=[False],
#    nvme_cmds=[False],
#    iopoll=[False],
#
#    num_threads=[1],
#    core_id=[64],
#    num_ssds=[8],
#    perfevent=[True],
#    write=[False, True],
#    duration=[10],  # in seconds
#    ssd_size=[1*TiB],
#    iodepth=[512],
#    bs=[4*KiB, 8*KiB, 16*KiB, 32*KiB, 64*KiB, 128*KiB, 256*KiB, 512*KiB, 1*MiB],
#    batch_submit=[32],
#    batch_complete=[32],
# )
# run(params)
# run(params.update(
#    reg_ring=[True],
# ))
# run(params.update(
#    reg_ring=[True],
#    reg_fds=[True],
# ))
# run(params.update(
#    reg_ring=[True],
#    reg_fds=[True],
#    reg_bufs=[True],
# ))
# run(params.update(
#    reg_ring=[True],
#    reg_fds=[True],
#    reg_bufs=[True],
#    nvme_cmds=[True],
# ))
# run(params.update(
#    reg_ring=[True],
#    reg_fds=[True],
#    reg_bufs=[True],
#    nvme_cmds=[True],
#    iopoll=[True],
# ))

params = ParameterGrid(
    csv_file='data/bench_ssd_scaleout.csv',
    run=range(1),

    mode=['defer'],
    posix=[False],
    libaio=[False],
    reg_ring=[False],
    reg_fds=[False],
    reg_bufs=[False],
    nvme_cmds=[False],
    iopoll=[False],

    num_threads=[1, 2, 4, 8, 16, 32, 64],
    core_id=[64],
    num_ssds=[8],
    perfevent=[False],
    write=[False, True],
    duration=[10],  # in seconds
    ssd_size=[1*TiB],
    iodepth=[512],
    bs=[4*KiB],
    batch_submit=[32],
    batch_complete=[32],
)
run(params)
run(params.update(
    libaio=[True],
))
run(params.update(
    reg_ring=[True],
))
run(params.update(
    reg_ring=[True],
    reg_fds=[True],
))
run(params.update(
    reg_ring=[True],
    reg_fds=[True],
    reg_bufs=[True],
))
run(params.update(
    reg_ring=[True],
    reg_fds=[True],
    reg_bufs=[True],
    nvme_cmds=[True],
))
run(params.update(
    reg_ring=[True],
    reg_fds=[True],
    reg_bufs=[True],
    nvme_cmds=[True],
    iopoll=[True],
))
run(params.update(
    reg_ring=[True],
    reg_fds=[True],
    reg_bufs=[True],
    nvme_cmds=[True],
    iopoll=[True],
    mode=['sqpoll'],
))
