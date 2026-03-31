from distexprunner import (
    Server,
    ServerList,
    ProcessGroup,
    ParameterGrid,
    CSVGenerator,
    ComputedParam,
    reg_exp,
    run_on_all,
    sleep,
)
from utils import (
    AttrDict,
    set_kernel_version,
    set_mtu,
    set_ssds,
    fmt_args,
    KiB,
    GiB,
)


SERVER_PORT = 20000
PROJECT_DIR = "~/ringding/"
CLEAN_BUILD = False
DEBUG = False
BIN = "remotescan"
CSV_FILE = "data/bench_remotescan.csv"
IFACE = "ens3np0"
SSD_MODEL = "KIOXIA"


server_list = ServerList(
    Server("fn01", "10.0.21.51", port=SERVER_PORT, ib_ip="192.168.1.11"),
    Server("fn03", "10.0.21.53", port=SERVER_PORT, ib_ip="192.168.1.13"),
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
reg_exp(
    servers=server_list.unique_by_ip, run_always=True, params=ParameterGrid(iface=IFACE)
)(set_mtu)
reg_exp(
    servers=server_list.unique_by_ip,
    run_always=True,
    params=ParameterGrid(name=SSD_MODEL),
)(set_ssds)


def cleanup(servers):
    servers.cd(PROJECT_DIR)
    run_on_all(servers, f"sudo pkill -f ./build/{BIN}", verify_rc=False)
    sleep(1)


SERVER_REGEX = (
    r"backend=\S+ "
    r"mode=server "
    r"num_workers=\d+ "
    r"time_s=\S+ "
    r"bytes=\d+ "
    r"ssd_read_bytes=\d+ "
    r"send_bytes=\d+ "
    r"recv_bytes=\d+ "
    r"bandwidth_Bps=(?P<server_bandwidth_Bps>\d+) "
    r"bandwidth_GiB_s=(?P<server_bandwidth_GiB_s>\S+)"
)

CLIENT_REGEX = (
    r"backend=\S+ "
    r"mode=client "
    r"num_workers=\d+ "
    r"time_s=\S+ "
    r"bytes=\d+ "
    r"ssd_read_bytes=\d+ "
    r"send_bytes=\d+ "
    r"recv_bytes=\d+ "
    r"bandwidth_Bps=(?P<client_bandwidth_Bps>\d+) "
    r"bandwidth_GiB_s=(?P<client_bandwidth_GiB_s>\S+)"
)


def bench_remotescan(
    servers,
    csv_file,
    run,
    backend,
    num_sockets,
    num_workers,
    num_ssds,
    request_size,
    stripe_size,
    reg_ring,
    reg_fds,
    reg_bufs,
    napi,
    send_zc,
    pin_queues,
    read_bytes=None,
    duration=None,
):
    cleanup(servers)
    servers.cd(PROJECT_DIR)
    run_on_all(servers, "./script/clear_ntuple.sh", verify_rc=False)
    run_on_all(servers, f"src/shuffle/prepare.sh --iface {IFACE}")

    server = servers[0]
    client = servers[1]

    has_read_bytes = read_bytes not in ("", None)
    has_duration = duration not in ("", None)
    assert has_read_bytes != has_duration, (
        "Exactly one of read_bytes or duration must be set"
    )

    common_args = AttrDict(
        backend=backend,
        num_sockets=num_sockets,
        num_workers=num_workers,
        request_size=request_size,
        stripe_size=stripe_size,
        reg_ring=reg_ring,
        reg_fds=reg_fds,
        reg_bufs=reg_bufs,
        napi=napi,
        pin_queues=pin_queues,
    )
    if has_read_bytes:
        common_args.scan_bytes = read_bytes
    if has_duration:
        common_args.duration = duration

    csv = CSVGenerator(
        SERVER_REGEX,
        CLIENT_REGEX,
        run=run,
        backend=backend,
        kernel=server.kernel,
        mtu=server.mtu,
        read_bytes=read_bytes if has_read_bytes else "",
        duration=duration if has_duration else "",
        num_sockets=num_sockets,
        num_workers=num_workers,
        num_ssds=num_ssds,
        request_size=request_size,
        stripe_size=stripe_size,
        reg_ring=reg_ring,
        reg_fds=reg_fds,
        reg_bufs=reg_bufs,
        napi=napi,
        send_zc=send_zc,
        pin_queues=pin_queues,
        server=server.id,
        client=client.id,
    )
    server_args = AttrDict(
        mode="server",
        ip=server.ib_ip,
        ssds=server.ssds[:num_ssds],
        send_zc=send_zc,
        **common_args,
    )

    client_args = AttrDict(
        mode="client",
        ip=server.ib_ip,
        **common_args,
    )

    procs = ProcessGroup()
    server_cmd = f'sudo bash -c "ulimit -n 4096; ./build/{BIN} {fmt_args(server_args)}"'
    client_cmd = f'sudo bash -c "ulimit -n 4096; ./build/{BIN} {fmt_args(client_args)}"'

    procs.add(server.run_cmd(server_cmd, stdout=[csv], timeout=300))
    sleep(1)
    procs.add(client.run_cmd(client_cmd, stdout=[csv], timeout=300))
    procs.wait()

    csv.write(csv_file)


def run(params):
    reg_exp(servers=server_list, params=params, raise_on_rc=False)(bench_remotescan)


params = ParameterGrid(
    csv_file=CSV_FILE,
    run=range(1),
    backend=["io_uring", "epoll_libaio"],
    duration=[30_000],
    # num_sockets=[1, 2, 4, 8, 16, 32],
    num_sockets=[32],
    num_workers=[1, 2, 4, 8, 16, 32, 64],
    num_ssds=[8],
    request_size=[4 * KiB, 16 * KiB, 64 * KiB, 128 * KiB, 512 * KiB],
    # request_size=[4 * KiB, 16 * KiB, 64 * KiB],
    stripe_size=ComputedParam(lambda request_size: request_size),
    reg_ring=[False],
    reg_fds=[False],
    reg_bufs=[False],
    napi=[False],
    send_zc=[False],
    pin_queues=[False],
)
run(params)

# uring optimizations
params = params.update(
    backend=["io_uring"],
)

run(
    params.update(
        reg_ring=[True],
        reg_fds=[True],
        reg_bufs=[True],
        napi=[False],
        send_zc=[False],
    )
)

run(
    params.update(
        reg_ring=[True],
        reg_fds=[True],
        reg_bufs=[True],
        napi=[False],
        send_zc=[True],
    )
)
