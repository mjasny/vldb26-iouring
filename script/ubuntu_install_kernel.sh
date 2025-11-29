#!/bin/bash
set -e

REPO_URL="${REPO_URL:-https://github.com/torvalds/linux.git}"
BRANCH="${BRANCH:-master}"
KERNEL_SRC="${KERNEL_SRC:-linux}"
MAKE_PROCS="${MAKE_PROCS:-$(nproc)}"

# Install necessary packages
#sudo apt update
sudo apt install -y htop tmux libssl-dev build-essential git fakeroot libelf-dev libdw-dev


# Set GRUB timeout
sudo sed -i 's/^GRUB_TIMEOUT=.*/GRUB_TIMEOUT=10/' /etc/default/grub


# Clone or update the Linux kernel repository
if [ ! -d "$KERNEL_SRC" ]; then
    echo "Cloning repository..."
    git clone --depth 1 --branch "$BRANCH" "$REPO_URL" "$KERNEL_SRC"
    # Ensure future fetches can see all branches
    pushd "$KERNEL_SRC" >/dev/null
    git config remote.origin.fetch "+refs/heads/*:refs/remotes/origin/*"
    popd >/dev/null
else
    echo "Updating repository..."
    if [ ! -d "$KERNEL_SRC/.git" ]; then
        echo "Directory exists but is not a git repository. Backing up and recloning..."
        mv "$KERNEL_SRC" "${KERNEL_SRC}.bak.$(date +%s)"
        git clone --depth 1 --branch "$BRANCH" "$REPO_URL" "$KERNEL_SRC"
        pushd "$KERNEL_SRC" >/dev/null
        git config remote.origin.fetch "+refs/heads/*:refs/remotes/origin/*"
        popd >/dev/null
    else
        pushd "$KERNEL_SRC" >/dev/null

        # Ensure origin URL
        CURRENT_URL="$(git config --get remote.origin.url || true)"
        if [ "$CURRENT_URL" != "$REPO_URL" ]; then
            echo "Remote URL differs (current: $CURRENT_URL). Switching origin to $REPO_URL"
            git remote set-url origin "$REPO_URL"
        fi

        # Make sure we fetch ALL branches, not just the initial shallow one
        git config remote.origin.fetch "+refs/heads/*:refs/remotes/origin/*"

        # Decide whether to keep fetch shallow
        if [ "$(git rev-parse --is-shallow-repository 2>/dev/null || echo false)" = "true" ]; then
            DEPTH_ARGS=(--depth=1)
        else
            DEPTH_ARGS=()
        fi

        # Fetch the target branch explicitly (creates refs/remotes/origin/$BRANCH)
        if ! git fetch --prune origin "refs/heads/$BRANCH:refs/remotes/origin/$BRANCH" "${DEPTH_ARGS[@]}"; then
            echo "Error: failed to fetch branch '$BRANCH' from '$REPO_URL'."
            popd >/dev/null
            exit 1
        fi

        # Verify the branch now exists on origin
        if ! git show-ref --verify --quiet "refs/remotes/origin/$BRANCH"; then
            echo "Error: branch '$BRANCH' not found on remote '$REPO_URL'."
            popd >/dev/null
            exit 1
        fi

        # Create/switch local branch to track remote and match it exactly
        if git rev-parse --verify "$BRANCH" >/dev/null 2>&1; then
            git checkout "$BRANCH"
            git reset --hard "origin/$BRANCH"
        else
            git checkout -B "$BRANCH" --track "origin/$BRANCH"
        fi

        popd >/dev/null
    fi
fi


#rm -rf linux-*.{deb,tar.gz,changes,buildinfo,dsc}

pushd $KERNEL_SRC

# Set up the kernel configuration
cp -v /boot/config-$(uname -r) .config
#sed -i 's/^CONFIG_DEBUG_INFO_BTF=.*/CONFIG_DEBUG_INFO_BTF=n/' .config

scripts/config --disable SYSTEM_TRUSTED_KEYS
scripts/config --disable SYSTEM_REVOCATION_KEYS

# Fix ZSTD-related signing error
scripts/config --disable MODULE_COMPRESS_ZSTD
scripts/config --enable MODULE_COMPRESS_NONE

# Update configuration based on previous config
yes "" | make olddefconfig

KERNEL_VERSION=$(make kernelrelease)
echo "Kernel version is $KERNEL_VERSION"

# Build the kernel and create Debian packages
echo "Building the kernel and creating Debian packages..."
make -j $MAKE_PROCS deb-pkg

# Go back to the parent directory where the .deb packages were generated
popd

# Install the generated .deb packages
echo "Installing the kernel and headers from the generated .deb packages..."

image_deb=($(ls ./linux-image-${KERNEL_VERSION}_*.deb | grep -v "dbg"))
headers_deb=($(ls ./linux-headers-${KERNEL_VERSION}_*.deb))
version_prefix=${KERNEL_VERSION//+}
libc_dev_deb=($(ls ./linux-libc-dev_${version_prefix//-/'[-~]'}*.deb 2>/dev/null))

sudo dpkg -i "${image_deb[@]}" "${headers_deb[@]}" "${libc_dev_deb[@]}"



echo "Updating GRUB with the new kernel..."
sudo update-grub

echo "Kernel installation complete. Reboot to use the new kernel."

dpkg --list | grep linux-image


exit 0

# Install perf
sudo apt remove linux-tools*
sudo apt install libtraceevent-dev libslang2-dev libperl-dev systemtap-sdt-dev python-dev-is-python3
cd linux/tools/perf
make -j
#sudo make install
sudo cp perf /usr/bin/perf
