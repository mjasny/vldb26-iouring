from distexprunner import (
    Action, Server, ServerList,  ProcessGroup, IterClassGen, ParameterGrid,
    CSVGenerator, ComputedParam, Action, SubstrMatcher,
    reg_exp, run_on_all, log, sleep
)
from utils import (
    StdoutBuffer, AttrDict, StatsAggr, PerfOut,
    set_mitigations, set_kernel_version, set_mtu,
    fmt_args, calculate_statistics, prefix_keys, KiB, MiB
)
import signal


SERVER_PORT = 20000
PROJECT_DIR = '~/ringding/'
CLEAN_BUILD = False
DEBUG = False
BINS = ['epoll_server', 'epoll_client', 'server_bw', 'client_bw', 'zc_recv']


server_list = ServerList(
    # Server('fn01', '10.0.21.51', port=SERVER_PORT, ib_ip='192.168.1.11'),
    # Server('fn02', '10.0.21.52', port=SERVER_PORT, ib_ip='192.168.1.12'),
    # Server('fn02', '10.0.21.52', port=SERVER_PORT, ib_ip='192.168.1.12'),
    # Server('fn03', '10.0.21.53', port=SERVER_PORT, ib_ip='192.168.1.13'),
    Server('fn04', '10.0.21.54', port=SERVER_PORT, ib_ip='192.168.1.14'),
    # Server('fn05', '10.0.21.55', port=SERVER_PORT, ib_ip='192.168.1.15'),
    Server('fn06', '10.0.21.56', port=SERVER_PORT, ib_ip='192.168.1.16'),
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
    run_on_all(servers, f'make -C build/ -j {" ".join(BINS)}')


reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_kernel_version)
reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_mitigations)

reg_exp(servers=server_list.unique_by_ip, run_always=True,
        params=ParameterGrid(iface='ens3np0'))(set_mtu)


@reg_exp(servers=server_list, run_always=True, raise_on_rc=False)
def pkill(servers):
    for bin in BINS:
        run_on_all(servers, f'sudo pkill -f ./build/{bin}', verify_rc=False)


def flag_to_arg(args, flags):
    MAP = (
        ('R', 'reg_ring'),
        ('F', 'reg_fds'),
        ('B', 'reg_bufs'),
        ('N', 'napi'),
        ('M', 'mshot_recv'),
        ('Z', 'send_zc'),
        ('O', 'recv_bundle'),
        ('H', 'hugepages'),
    )
    for flag, field in MAP:
        # args[field] = flag in flags
        if flag in flags:
            args[field] = True


def bench_zc_threshold(servers, csv_file, run, duration, connections, workload, flags, **kwargs):
    servers.cd(PROJECT_DIR)
    run_on_all(servers, 'src/shuffle/prepare.sh')

    csv = CSVGenerator(
        run=run,
        duration=duration,
        connections=connections,
        workload=workload,
        flags=flags,
    )

    procs = ProcessGroup()
    args = AttrDict(
        **kwargs,
        # perfevent=True,
    )

    # if 'H' in flags or args.hugepages:
    #    n_pages = 128 * connections
    # else:
    #    n_pages = 0
    n_pages = 128 * connections
    cmd = f'echo {n_pages} | sudo tee ' + \
        '/sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages'
    run_on_all(servers, cmd)

    # Server
    s = servers[0]
    csv.add_columns(
        kernel=s.kernel,
        mitigations=s.mitigations,
        mtu=s.mtu,
        node=s.id,
        **args,
    )

    # args.hugepages = True

    server_done = SubstrMatcher('init done')

    perf = PerfOut()
    stats = StatsAggr()
    s_args = AttrDict(args)
    s_args.stop_after_last = True
    server_bin = 'server_bw'
    if workload == 'send':
        s_args.setup_mode = 'defer'
        s_args.num_threads = connections  # have as many server threads as connections
        s_args.size = MiB * 8
    else:
        s_args.perfevent = True
        s_args.setup_mode = 'defer'
        if 'Z' in flags:
            server_bin = 'zc_recv'
            # make -j zc_recv && sudo ./zc_recv --ip 192.168.1.14 --ifname ens3np0 --queue_id 4 --perfevent true --size 256 --nr_conns 1
            s_args.ifname = 'ens3np0'
            s_args.queue_id = s_args.core_id + 1
            s_args.nr_conns = connections

            assert (s_args.num_threads == 1)
            for x in ['num_threads', 'pin_queues', 'stop_after_last']:
                del s_args[x]

            sleep(1)
            s.run_cmd('sudo ethtool -K ens3np0 rx-gro-hw on').wait()
            s.run_cmd('sudo ethtool -G ens3np0 tcp-data-split on').wait()
            s.run_cmd('sudo ethtool -X ens3np0 equal 1').wait()
            sleep(3)

        else:
            flag_to_arg(s_args, flags)
    cmd = f'sudo ./build/{server_bin} --ip {s.ib_ip} {fmt_args(s_args)}'
    stdout = [csv, perf, server_done]
    if workload == 'recv':
        stdout += [stats]
    procs.add(s.run_cmd(cmd, stdout=stdout))

    sleep(1)
    server_done.wait()
    stats._lines.clear()

    assert (connections <= 64)
    # Client
    c = servers[1]
    c_args = AttrDict(args)
    c_args.duration = duration
    client_bin = 'client_bw'
    if workload == 'recv':
        c_args.num_threads = connections
        c_args.size = MiB * 8
        c_args.send_zc = True
        c_args.reg_bufs = True
        c_args.reg_fds = True
        c_args.reg_ring = True
    else:
        c_args.perfevent = True
        c_args.num_threads = 1
        c_args.conn_per_thread = connections
        s_args.setup_mode = 'defer'
        flag_to_arg(c_args, flags)
    cmd = f'sudo ./build/{client_bin} --ip {s.ib_ip} {fmt_args(c_args)}'
    stdout = [perf]
    if workload == 'send':
        stdout += [stats]
    procs.add(c.run_cmd(cmd, stdout=stdout))

    procs.wait()

    data = stats.get_data()
    bw = [x.bw for x in data if 'bw' in x]
    stats = calculate_statistics(bw, 10)

    csv.add_columns(**prefix_keys(stats, 'bw_'))

    csv.add_columns(**perf.get_data())
    csv.write(csv_file)
    sleep(1)


def run(params):
    reg_exp(servers=server_list, params=params,
            raise_on_rc=False)(bench_zc_threshold)


RUNS = 3
params = ParameterGrid(
    csv_file='data/bench_zc_threshold.csv',
    run=range(RUNS),

    workload=['send'],

    core_id=[3],
    duration=[30_000],
    num_threads=[1],
    # connections=[1, 4, 8, 16],
    size=[64, 128, 256, 512, 1*KiB, 2*KiB, 4*KiB, 8*KiB, 16*KiB,
          32*KiB, 64*KiB, 128*KiB, 256*KiB, 512*KiB, 1*MiB],
    flags=['------', 'RF----', 'RF---Z', 'RFB--Z'],  # + ['RFB--ZN'],
    connections=[16],  # otherwise receiver is too slow, spawns 16 recv threads
    pin_queues=[True],
)
# run(params)


RUNS = 3
params = ParameterGrid(
    csv_file='data/bench_zc_threshold.csv',
    run=range(RUNS),

    workload=['recv'],

    core_id=[3],
    duration=[30_000],
    num_threads=[1],
    size=[64, 128, 256, 512, 1*KiB, 2*KiB, 4*KiB, 8*KiB, 16*KiB,
          32*KiB, 64*KiB, 128*KiB, 256*KiB, 512*KiB, 1*MiB][::-1],
    # + ['RFN---', 'RFBN--', 'RFBMN-'], # recv_bundles? measure NAPI?
    flags=['------', 'RF----', 'RFB---', 'RFBM--'],
    connections=[1],
    pin_queues=[True],
)
# run(params)


RUNS = 3
params = ParameterGrid(
    csv_file='data/bench_zc_threshold.csv',
    run=range(RUNS),

    workload=['recv'],

    core_id=[3],
    duration=[30_000],
    num_threads=[1],
    # size=[64, 128, 256, 512, 1*KiB, 2*KiB, 4*KiB, 8*KiB, 16*KiB,
    #      32*KiB, 64*KiB, 128*KiB, 256*KiB, 512*KiB, 1*MiB][::-1],
    size=[64, 128],
    flags=['R---Z'],
    connections=[1],
    pin_queues=[True],
)
run(params)
