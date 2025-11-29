from distexprunner import (
    Action, Server, ServerList,  ProcessGroup, IterClassGen, ParameterGrid,
    CSVGenerator, ComputedParam, Action, ReturnCode, File,
    reg_exp, run_on_all, log, sleep
)
from utils import (
    StdoutBuffer, AttrDict, StatsAggr,
    set_kernel_version, set_mitigations, set_mtu,
    calculate_statistics, prefix_keys,
    fmt_args, PerfOut, PerfParser
)
import time


SERVER_PORT = 20000
PROJECT_DIR = '~/ringding/'
CLEAN_BUILD = False
DEBUG = False
BIN = 'shufflev2'


server_list = ServerList(
    Server('fn05', '10.0.21.55', port=SERVER_PORT, ib_ip='192.168.1.15'),
    Server('fn06', '10.0.21.56', port=SERVER_PORT, ib_ip='192.168.1.16'),
    Server('fn03', '10.0.21.53', port=SERVER_PORT, ib_ip='192.168.1.13'),
    Server('fn04', '10.0.21.54', port=SERVER_PORT, ib_ip='192.168.1.14'),
    Server('fn02', '10.0.21.52', port=SERVER_PORT, ib_ip='192.168.1.12'),
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


reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_kernel_version)
reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_mitigations)
reg_exp(servers=server_list.unique_by_ip, run_always=True,
        params=ParameterGrid(iface='ens3np0'))(set_mtu)


class DoneCounter:
    def __init__(self, key, callback=None):
        self._key = key
        self._callback = callback
        self.count = 0

    def __call__(self, line):
        if self._key in line:
            self.count += 1
            if self._callback:
                self._callback(self.count)


def bench_shufflev2(servers, csv_file, fairness, run, csv_file_membw=None, **kwargs):
    servers.cd(PROJECT_DIR)

    cmd = 'sudo python ./script/remap_nic_irqs.py c1:00.0'
    run_on_all(servers, cmd)

    run_on_all(servers, f'sudo pkill -f ./build/{BIN}', verify_rc=False)
    run_on_all(servers, './script/clear_ntuple.sh', verify_rc=False)
    cmd = 'src/shuffle/prepare.sh --iface ens3np0'
    if fairness:
        cmd += ' --for_fairness'
    run_on_all(servers, cmd)

    if csv_file_membw is not None:
        run_on_all(servers, 'sudo modprobe amd_uncore')

    if kwargs['recv_zc']:
        sleep(1)
        run_on_all(servers, 'sudo ethtool -K ens3np0 rx-gro-hw on')
        run_on_all(servers, 'sudo ethtool -G ens3np0 tcp-data-split on')
        run_on_all(servers, 'sudo ethtool -X ens3np0 equal 1')
        sleep(3)

    sleep(1)

    csvs = IterClassGen(
        CSVGenerator,
        run=run,
        fairness=fairness,
        **kwargs,
    )

    cmd = f'echo {350_000} | ' + \
        'sudo tee /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages'
    run_on_all(servers, cmd)

    num_nodes = kwargs.pop('num_nodes')
    assert (num_nodes <= len(servers))
    servers = servers[:num_nodes]
    ips = [s.ib_ip for s in servers]

    stats = IterClassGen(StatsAggr)

    done = 0

    def callback(count):
        nonlocal done
        total = kwargs["num_workers"]*num_nodes
        log(f"Done: {count}/{total}")
        done = count

    done_cntr = DoneCounter('Scan took', callback)

    membw_key = str(int(time.time())) if csv_file_membw else None

    global port
    port += 100

    procs = ProcessGroup()
    for i, s in enumerate(servers):
        csv = next(csvs)
        csv.add_columns(
            kernel=s.kernel,
            mitigations=s.mitigations,
            mtu=s.mtu,
            node=s.id,
            membw_key=membw_key,
        )

        args = AttrDict(
            port=port,
            my_id=i,
            ips=ips,
            ** kwargs,
        )
        # if num_nodes == 6 and args.tuple_size == 4096:
        #    args.hashtable_factor = 4.0

        if args.tuple_size in [16, 32]:
            args.scan_size //= 4

        if args.recv_zc:
            args.ifname = 'ens3np0'
        cmd = f'sudo bash -c "ulimit -n 4096; ./build/{BIN} {fmt_args(args)}"'
        # cmd = f'sudo ./build/{BIN} {fmt_args(args)}'
        stdout = [csv, next(stats), done_cntr]
        timeout = 600 if args.recv_zc else 120
        procs.add(s.run_cmd(cmd, stdout=stdout, timeout=timeout))

    perfs = IterClassGen(PerfParser)
    if csv_file_membw is not None:
        for s in servers:
            cmd = 'sudo perf stat -I 100 -a -x, --metric-only -M umc_mem_read_bandwidth -M umc_mem_write_bandwidth -M umc_data_bus_utilization -- bash -c "while pgrep -x shufflev2 >/dev/null; do sleep 1; done"'

            perf = next(perfs)
            perf.add_columns(
                membw_key=membw_key,
                node=s.id,
            )
            procs.add(s.run_cmd(cmd, stderr=[perf]))

    # procs.wait()
    if args.recv_zc:
        while done == 0:
            if procs.has_timeout(block=False):
                break
            rcs = procs.wait(block=False)
            if all(rc is not None for rc in rcs):
                break
            sleep(1)

        cmd = f'sudo pkill --signal SIGUSR1 -f ./build/{BIN}'
        run_on_all(servers, cmd, verify_rc=False)
        sleep(1)
        procs.wait(verify_rc=False)

    else:
        rcs = procs.wait(verify_rc=False)
        if any(x == ReturnCode.TIMEOUT for x in rcs):
            sleep(1)
            return Action.RESTART
        procs.wait()  # Final check

    for perf in perfs:
        perf.write(csv_file_membw)

    for csv, stat in zip(csvs, stats):
        stat.write(csv_file, csv=csv)


def run(params):
    reg_exp(servers=server_list, params=params,
            raise_on_rc=False)(bench_shufflev2)


KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB

RUNS = 1
params = ParameterGrid(
    csv_file='data/bench_shufflev2.csv',
    run=range(RUNS),

    scan_size=[100*GiB],
    tuple_size=[128],
    num_nodes=[2],
    num_workers=[1],
    nr_conns=[1],
    pin_queues=[True],
    reg_ring=[False],
    reg_fds=[False],
    reg_bufs=[False],
    send_zc=[False],
    recv_zc=[False],
    napi=[False],

    fairness=[True],
    same_irq=ComputedParam(lambda fairness: [fairness]),
    use_budget=[False],
    use_hashtable=[True],
    stats_interval=[100_000],
)

# # For Microbenchmark
# run(params.update(
#     tuple_size=[128, 256, 512, 1024],
#     nr_conns=[1],
#     fairness=[False, True],
#     num_workers=[1, 4, 8, 16, 32],
#     use_hashtable=[True, False],
# ))

# port = 25000 + 3800
# basic = params.update(
#    csv_file='data/bench_shufflev2_1.csv',
#    csv_file_membw='data/bench_shufflev2_1_membw.csv',
#    # tuple_size=[128, 512, 256, 1024],
#    tuple_size=[16, 64, 4096, 16384],
#    nr_conns=[1],
#    fairness=[True],
#    num_workers=[1, 2, 4, 8, 16, 32][::-1],
#    use_hashtable=[True, False][:1],
#    reg_ring=[True],
#    reg_fds=[True],
#    reg_bufs=[False],
#    send_zc=[False],
#    num_nodes=[6],
# )
# run(basic)
# run(basic.update(
#    send_zc=[True],
# ))
# run(basic.update(
#    send_zc=[True],
#    recv_zc=[True],
# ))


# # multi-threaded
# run(params.update(
#     tuple_size=[128, 256, 512, 1024],
#     nr_conns=[1],
#     fairness=[False, True],
#     num_workers=[16, 32],
#     use_hashtable=[True, False],
#     reg_ring=[True],
#     reg_fds=[True],
#     reg_bufs=[True],
#     send_zc=[True],
# ))
#
#
# # scaleout
# run(params.update(
#     tuple_size=[128, 256, 512, 1024],
#     nr_conns=[1],
#     fairness=[True],
#     num_workers=[16, 32],
#     use_hashtable=[True, False],
#     reg_ring=[True],
#     reg_fds=[True],
#     reg_bufs=[True],
#     send_zc=[True],
#     num_nodes=[4, 5],
# ))


# port = 25000
# # Debug
# run(params.update(
#     csv_file='data/bench_shufflev2_debug2.csv',
#     csv_file_membw='data/bench_shufflev2_debug2_membw.csv',
#     tuple_size=[128, 256, 512, 1024],
#     nr_conns=[1],
#     fairness=[True],
#     num_workers=[1, 2, 4, 8, 16, 32],
#     # num_workers=[8, 16, 32][::-1],
#     use_hashtable=[False, True],
#     reg_ring=[True],
#     reg_fds=[True],
#     reg_bufs=[True],
#     send_zc=[True],
#     recv_zc=[False, True][:1],
#     num_nodes=[2, 3, 4, 5, 6],
# ))
#
# run(params.update(
#     csv_file='data/bench_shufflev2_debug2.csv',
#     csv_file_membw='data/bench_shufflev2_debug2_membw.csv',
#     tuple_size=[128, 256, 512, 1024],
#     nr_conns=[1],
#     fairness=[True],
#     num_workers=[1, 2, 4, 8, 16, 32],
#     use_hashtable=[False, True],
#     reg_ring=[False],
#     reg_fds=[False],
#     reg_bufs=[False],
#     send_zc=[False],
#     recv_zc=[False],
#     num_nodes=[2, 3, 4, 5, 6],
# ))


# port = 25000 + 4000
#
# basic = params.update(
#    csv_file='data/bench_shufflev2_2.csv',
#    tuple_size=[128, 256, 512, 1024],
#    nr_conns=[1],
#    fairness=[True],
#    num_workers=[8, 16, 32],
#    use_hashtable=[True, False],
#    reg_ring=[False],
#    reg_fds=[False],
#    reg_bufs=[False],
#    send_zc=[False],
#    num_nodes=[6],
# )
# run(basic)
# opti = basic.update(
#    reg_ring=[True],
#    reg_fds=[True],
#    reg_bufs=[True],
#    send_zc=[True],
# )
# run(opti)
# run(opti.update(
#    recv_zc=[True],
# ))

port = 20100

basic = params.update(
    csv_file='data/bench_shufflev2_3.csv',
    #    tuple_size=[128, 256, 512, 1024],
    tuple_size=[16, 32, 64, 2048, 4096, 8192, 16384],
    nr_conns=[1],
    fairness=[True],
    num_workers=[1, 8, 16, 32],
    use_hashtable=[True, False][:1],
    reg_ring=[False],
    reg_fds=[False],
    reg_bufs=[False],
    send_zc=[False],
    num_nodes=[6],
)
run(basic.update(
    use_epoll=[True],
    send_zc=[False, True],
))
run(basic.update(
    use_epoll=[False],
    reg_ring=[False],
    reg_fds=[False],
    reg_bufs=[False],
    send_zc=[False],
    recv_zc=[False],
))
run(basic.update(
    use_epoll=[False],
    reg_ring=[True],
    reg_fds=[True],
    reg_bufs=[True],
    send_zc=[True],
    recv_zc=[False, True],
))
