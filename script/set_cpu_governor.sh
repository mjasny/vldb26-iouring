#/bin/bash

cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_available_governors

# Default is schedutil
echo "Before: "
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor | sort | uniq -c

for CPU in /sys/devices/system/cpu/cpu[0-9]*; do
    echo performance | sudo tee $CPU/cpufreq/scaling_governor >/dev/null
done

echo "After: "
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor | sort | uniq -c
