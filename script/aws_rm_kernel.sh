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
sudo rm -v /boot/initramfs-$KERNEL_VERSION.img
sudo rm -v /boot/System.map-$KERNEL_VERSION
sudo rm -v /boot/config-$KERNEL_VERSION

# Remove kernel modules from /lib/modules
echo "Removing kernel modules from /lib/modules..."
sudo rm -rf /lib/modules/$KERNEL_VERSION

# Remove kernel headers from /usr/src
echo "Removing kernel headers from /usr/src..."
sudo rm -rf /usr/src/linux-headers-$KERNEL_VERSION

# Update the bootloader using grub-mkconfig
echo "Updating the bootloader with grub-mkconfig..."
sudo grub2-mkconfig -o /boot/grub2/grub.cfg

# Optionally, remove the kernel from grubby if it was added
if sudo grubby --info=ALL | grep -q "/boot/vmlinuz-$KERNEL_VERSION"; then
    echo "Removing kernel from grubby..."
    sudo grubby --remove-kernel=/boot/vmlinuz-$KERNEL_VERSION
fi

echo "Kernel $KERNEL_VERSION removed successfully. Please reboot your system."

