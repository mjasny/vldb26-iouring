#!/bin/bash
set -e

# Define the kernel version to be removed
KERNEL_VERSION="$1"

if [ -z "$KERNEL_VERSION" ]; then
    echo "Usage: $0 <kernel-version>"
    exit 1
fi

# Remove kernel files from /boot
echo "Removing kernel files from /boot..."
sudo rm -v /boot/vmlinuz-$KERNEL_VERSION
sudo rm -v /boot/initrd.img-$KERNEL_VERSION
sudo rm -v /boot/System.map-$KERNEL_VERSION
sudo rm -v /boot/config-$KERNEL_VERSION

# Remove kernel modules from /lib/modules
echo "Removing kernel modules from /lib/modules..."
sudo rm -rf /lib/modules/$KERNEL_VERSION

# Remove kernel headers from /usr/src
echo "Removing kernel headers from /usr/src..."
sudo rm -rf /usr/src/linux-headers-$KERNEL_VERSION

# Update the bootloader with update-grub
echo "Updating the bootloader..."
sudo update-grub

echo "Kernel $KERNEL_VERSION removed successfully. Please reboot your system if necessary."

