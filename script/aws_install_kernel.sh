#!/bin/bash
set -e

REPO_URL="${REPO_URL:-https://github.com/torvalds/linux.git}"
BRANCH="${BRANCH:-master}"
KERNEL_SRC="${KERNEL_SRC:-linux}"
MAKE_PROCS="${MAKE_PROCS:-$(nproc)}"


sudo yum install -y htop tmux openssl-devel
sudo yum groupinstall -y "Development Tools"


sudo sed -i 's/^GRUB_TIMEOUT=.*/GRUB_TIMEOUT=10/' /etc/default/grub


# Check if the directory exists
if [ ! -d "$KERNEL_SRC" ]; then
    echo "Cloning repository..."
    git clone --depth 1 --branch $BRANCH $REPO_URL $KERNEL_SRC
else
    echo "Updating repository..."
    pushd $KERNEL_SRC
    CURRENT_URL=$(git config --get remote.origin.url)
    
    if [ "$CURRENT_URL" == "$REPO_URL" ]; then
        git pull origin $BRANCH
    else
        echo "Remote URL does not match. Exiting."
        popd
        exit 1
    fi
    popd
fi

pushd $KERNEL_SRC

# sudo make mrproper

cp -v /boot/config-$(uname -r) .config
sed -i 's/^CONFIG_DEBUG_INFO_BTF=.*/CONFIG_DEBUG_INFO_BTF=n/' .config

if grep -q "^CONFIG_NET_VENDOR_AMAZON" .config; then
    sed -i "s/^CONFIG_NET_VENDOR_AMAZON=.*/CONFIG_NET_VENDOR_AMAZON=y/" .config
else
    echo "CONFIG_NET_VENDOR_AMAZON=y" >> .config
fi
if grep -q "^CONFIG_ENA_ETHERNET" .config; then
    sed -i "s/^CONFIG_ENA_ETHERNET=.*/CONFIG_ENA_ETHERNET=m/" .config
else
    echo "CONFIG_ENA_ETHERNET=m" >> .config
fi

yes "" | make olddefconfig

echo "Building the kernel..."
make -j $MAKE_PROCS

KERNEL_VERSION=$(make kernelrelease)

echo "Installing the modules..."
sudo make modules_install

#echo "Installing the kernel..."
#sudo make install INSTALL_PATH=/boot # Fails because of LILO?

echo "Installing the kernel manually..."
sudo cp -v arch/x86/boot/bzImage /boot/vmlinuz-$KERNEL_VERSION
sudo cp -v System.map /boot/System.map-$KERNEL_VERSION
sudo cp -v .config /boot/config-$KERNEL_VERSION

echo "Creating initramfs..."
sudo dracut -f /boot/initramfs-$KERNEL_VERSION.img $KERNEL_VERSION

echo "Installing the kernel headers for version $KERNEL_VERSION..."
sudo make headers_install INSTALL_HDR_PATH=/usr/src/linux-headers-$KERNEL_VERSION

popd

if ls /lib/modules/$KERNEL_VERSION/kernel/drivers/net/ethernet/amazon/ena/ena.ko &>/dev/null; then
    echo "ENA driver built successfully."
else
    echo "ENA driver was not built."
    exit 1
fi

echo "Adding new kernel to GRUB..."
sudo grubby --add-kernel=/boot/vmlinuz-$KERNEL_VERSION --initrd=/boot/initramfs-$KERNEL_VERSION.img --title="Linux $KERNEL_VERSION" --args="ro quiet"

# BOOT_IMAGE=(hd0,gpt1)/boot/vmlinuz-6.1.97-104.177.amzn2023.x86_64 root=UUID=f47164d0-c650-4a0f-8f35-968ce4cf18a1 ro console=tty0 console=ttyS0,115200n8 nvme_core.io_timeout=4294967295 rd.emergency=poweroff rd.shell=0 selinux=1 security=selinux quiet


echo "Updating the bootloader..."
sudo grubby --set-default /boot/vmlinuz-$KERNEL_VERSION
sudo grub2-mkconfig -o /boot/grub2/grub.cfg

sudo grubby --info=ALL | grep -E "^kernel|^index"
