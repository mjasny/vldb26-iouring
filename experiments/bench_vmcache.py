from distexprunner import (
    Action, Server, ServerList,  ProcessGroup, IterClassGen, ParameterGrid,
    CSVGenerator, ComputedParam, Action,
    reg_exp, run_on_all, log, sleep
)
from utils import (
    StdoutBuffer, AttrDict, CSVOutput,
    set_kernel_version, set_mitigations, set_ssds,
    fmt_args, PerfOut
)
from collections.abc import Iterable

SERVER_PORT = 20000
PROJECT_DIR = '~/ringding/vmcache'
CLEAN_BUILD = False
DEBUG = False
BIN = 'vmcache'


server_list = ServerList(
    Server('fn01', '10.0.21.51', port=SERVER_PORT, ib_ip='192.168.1.11'),
)


@reg_exp(servers=server_list.unique_by_ip, run_always=True)
def compile(servers):
    servers.cd(PROJECT_DIR)
    if CLEAN_BUILD:
        run_on_all(servers, 'make clean')
    run_on_all(servers, f'make -j {BIN}')


reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_kernel_version)
reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_mitigations)
reg_exp(servers=server_list.unique_by_ip, run_always=True)(set_ssds)


@reg_exp(servers=server_list, run_always=True, raise_on_rc=False)
def pkill(servers):
    run_on_all(servers, f'sudo pkill -f ./{BIN}', verify_rc=False)


def fmt_env(args):
    d = []
    for k, v in args.items():
        if isinstance(v, Iterable) and not isinstance(v, str):
            v = ','.join(map(str, v))
        d.append(f'{k.upper()}={v}')
    return ' '.join(d)


def bench_vmcache(servers, csv_file, ssd_id, run, **kwargs):
    servers.cd(PROJECT_DIR)

    csvs = IterClassGen(
        CSVGenerator,
        run=run,
        **kwargs,
    )

    outputs = IterClassGen(CSVOutput)

    procs = ProcessGroup()
    for s in servers:
        csv = next(csvs)
        ssd_path = s.ssds[ssd_id]
        csv.add_columns(
            kernel=s.kernel,
            mitigations=s.mitigations,
            node=s.id,
            ssd=ssd_path,
        )

        args = AttrDict(
            block=ssd_path,
            **kwargs,
        )

        cmd = f'sudo {fmt_env(args)} ./{BIN}'
        procs.add(s.run_cmd(cmd, stdout=[csv, next(outputs)]))

    procs.wait()

    for csv, output in zip(csvs, outputs):
        output.write(csv_file, csv=csv)


def run(params):
    reg_exp(servers=server_list, params=params,
            raise_on_rc=False)(bench_vmcache)


# make && sudo BLOCK=/dev/nvme8n1 THREADS=1 DATASIZE=100 PHYSGB=1 VIRTGB=32 RUNFOR=200 EXMAP=1 ./vmcache

RUNS = 1
params = ParameterGrid(
    csv_file='data/bench_vmcache2.csv',
    run=range(RUNS),
    ssd_id=[0],

    datasize=[1],
    batch=[128],
    threads=[1],
    physgb=[1],
    virtgb=[32],
    runfor=[200],

    exmap=[0],
)


# WH=1
run(params)

# WH=100
run(params.update(
    datasize=[100],
))
