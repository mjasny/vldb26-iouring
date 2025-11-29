# io_uring for High-Performance DBMSs: When and How to Use It

This repository contains the scripts, configurations, and plotting tooling required to reproduce all experiments reported in our paper.


## Run experiments


```bash
# Install Nix on all servers.
ssh <node>
./scripts/install_nix.sh


cd experiments/

# Start Servers (runs in screen, shell can be closed)
./start_server.sh <node>


# Run experiments
nix-shell ../distexprunner
distexp-client -vv bench_nop.py
```

CSV files with measurements are stored in `experiments/data/`


## Generate Plots

```bash
cd experiments/
nix-shell

for f in *.R; do
    Rscript $f;
done
```

PDFs are generated to `experiments/out/`


### Manually run experiment:

```bash
mkdir build/
cd build/
cmake ..
make -j <binary>
```


### Postgres

Create filesystem on SSD and mount it to `/mnt/raid`.
Apply `0001-uring-opts.patch` to `postgresql/`.
Use script `experiments/postgres/run_all.sh`.



## Hardware and OS Configuration

The experiments were executed on six identical cluster nodes with the following configuration:

```
CPU: AMD EPYC 9654P, 96 physical cores (192 hardware threads disabled, SMT off)
Memory: 768 GiB DDR5
SSDs: 8x KIOXIA KCMYXRUG7T68 (CM7-R Series) PCIe Gen 5 x4 (only on 1 machine)
NICs 400Gbit Mellanox Technologies MT2910 Family [ConnectX-7]

Operating System: Ubuntu Server 24.04 LTS
Kernel Version : Compiled from source: 6.15 and 6.17.0+
glibc Version  : 2.39
gcc Version    : 13.3.0
OFED:          : 24.10-1.1.4
```


Kernel installation

```bash
cd script/

# Use commit b988603f4f1e135f91789681fcbe076442cbc2c6 (with zero-copy recv bug-fixes)
REPO_URL=https://git.kernel.dk/linux.git BRANCH=for-next ./ubuntu_install_kernel.sh

# Select kernel for next boot:
./select_kernel.sh
```



