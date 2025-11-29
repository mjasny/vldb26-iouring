from distexprunner import (
    Action, Server, ServerList,  ProcessGroup, IterClassGen, ParameterGrid,
    CSVGenerator, ComputedParam, Action,
    reg_exp, run_on_all, log, sleep
)
from utils import (
    StdoutBuffer, AttrDict, StatsAggr,
    set_mitigations, set_kernel_version, set_mtu,
    fmt_args, calculate_statistics
)


SERVER_PORT = 20000
PROJECT_DIR = '~/ringding/'
CLEAN_BUILD = False
DEBUG = False
BINS = ['server', 'client']


server_list = ServerList(
    Server('fn05', '10.0.21.55', port=SERVER_PORT, ib_ip='192.168.1.15'),
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


@reg_exp(servers=server_list, run_always=True, raise_on_rc=False)
def pkill(servers):
    for bin in BINS:
        run_on_all(servers, f'sudo pkill -f ./build/{bin}', verify_rc=False)


reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_kernel_version)
reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_mitigations)
reg_exp(servers=server_list.unique_by_ip, run_always=True,
        params=ParameterGrid(iface='ens3np0'))(set_mtu)


@reg_exp(servers=server_list.unique_by_ip, run_always=True)
def prepare(servers):
    servers.cd(PROJECT_DIR)
    cmd = 'src/shuffle/prepare.sh'
    run_on_all(servers, cmd)


def bench_tcpudp_lat(servers, csv_file, run, **kwargs):
    servers.cd(PROJECT_DIR)
    run_on_all(servers, './script/clear_ntuple.sh', verify_rc=False)

    kwargs.pop('opti')

    csvs = IterClassGen(
        CSVGenerator,
        run=run,
        **kwargs
    )

    procs = ProcessGroup()

    args = AttrDict(
        **kwargs
    )

    # Server
    s = servers[0]
    csv = next(csvs)
    csv.add_columns(
        kernel=s.kernel,
        mitigations=s.mitigations,
        mtu=s.mtu,
        node=s.id,
    )

    cmd = f'sudo ./build/server --ip {s.ib_ip} {fmt_args(args)}'
    procs.add(s.run_cmd(cmd, stdout=[csv]))

    sleep(1)

    # Client
    c = servers[1]
    csv = next(csvs)
    csv.add_columns(
        kernel=c.kernel,
        mitigations=c.mitigations,
        mtu=s.mtu,
        node=c.id,
    )
    if not args.tcp:
        args.local_ip = c.ib_ip
    stats = StatsAggr()
    cmd = f'sudo ./build/client --ip {s.ib_ip} {fmt_args(args)}'
    procs.add(c.run_cmd(cmd, stdout=[csv, stats]))

    procs.wait()

    data = stats.get_data()
    latencies = [x.latency for x in data if 'latency' in x]
    stats = calculate_statistics(latencies, 10)

    for csv in csvs:
        csv.add_columns(**stats)

    for csv in csvs:
        csv.write(csv_file)


def run(params):
    reg_exp(servers=server_list, params=params,
            raise_on_rc=False)(bench_tcpudp_lat)


RUNS = 5
params = ParameterGrid(
    csv_file='data/bench_tcpudp_lat.csv',
    run=range(RUNS),

    core_id=[8],
    tcp=[False, True][1:],
    opti=[0, 1, 2, 3, 4],
    reg_ring=ComputedParam(lambda opti: opti >= 1),
    reg_fds=ComputedParam(lambda opti: opti >= 2),
    reg_bufs=ComputedParam(lambda opti: opti >= 3),
    napi=ComputedParam(lambda opti: opti >= 4),

    setup_mode=['sqpoll', 'defer'],
    ping_size=[8],
    resp_delay=[0],
    duration=[30_000],
    num_threads=[1],
    pin_queues=[True],
    rx_queue=[10, 18],  # core_id + 2 because sqpoll
)
run(params)
