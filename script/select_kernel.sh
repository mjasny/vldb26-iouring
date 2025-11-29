#!/bin/bash

# Function to list kernels and submenus
list_kernels() {
    sudo awk '
    BEGIN { submenu="" }
    /submenu/ {
        # Capture submenu title
        if (match($0, /submenu '\''([^'\'']+)'\''/, m)) {
            submenu=m[1]
        }
    }
    /menuentry/ {
        # Capture menuentry title
        if (match($0, /menuentry '\''([^'\'']+)'\''/, m)) {
            if (submenu != "") {
                print submenu ">" m[1]
            } else {
                print m[1]
            }
        }
    }
    ' /boot/grub/grub.cfg | grep -v 'recovery mode'
}

# List kernels
echo "Available kernels:"
list_kernels | nl


# Kernel can be passed as first CLI argument
selected_kernel="$1"

# If no argument was provided, ask the user
if [ -z "$selected_kernel" ]; then
    echo ""
    read -p "Enter part of the kernel version (or exact name) to boot: " selected_kernel
fi


# Find the GRUB entry matching the user input
grub_entry=$(list_kernels | grep -i "$selected_kernel")

if [[ -n "$grub_entry" ]]; then
    echo "Setting next boot to: $grub_entry"
    sudo grub-reboot "$grub_entry"
    echo "Reboot to boot into the selected kernel."
else
    echo "Kernel not found. Please try again."
fi
