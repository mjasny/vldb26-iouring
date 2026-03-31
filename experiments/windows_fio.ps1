$fio = "C:\Program Files\fio\fio.exe"
$outCsv = Join-Path (Get-Location).Path "fio_results.csv"

$drives = @(
    "\\.\PhysicalDrive0",
    "\\.\PhysicalDrive1",
    "\\.\PhysicalDrive2",
    "\\.\PhysicalDrive3",
    "\\.\PhysicalDrive4",
    "\\.\PhysicalDrive5",
    "\\.\PhysicalDrive6",
    "\\.\PhysicalDrive7"
)

$workloads = @("randread", "randwrite")
$threadsList = @(1, 2, 4, 8, 16, 32, 64)
$repetitions = 5

$size = "100G"
$runtime = 60
$bs = "4k"
$iodepth = 512

if (!(Test-Path $fio)) {
    Write-Error "fio not found at $fio"
    exit 1
}

"timestamp,workload,threads,run,read_iops,write_iops,read_bw_kib_s,write_bw_kib_s,read_clat_ns_mean,write_clat_ns_mean,read_lat_ns_mean,write_lat_ns_mean,exitcode" |
    Set-Content -Encoding ascii $outCsv

foreach ($workload in $workloads) {
    foreach ($threads in $threadsList) {
        foreach ($run in 1..$repetitions) {
            $timestamp = Get-Date -Format "yyyy-MM-ddTHH:mm:ss"
            Write-Host "[$timestamp] workload=$workload threads=$threads run=$run"

            $args = @(
                "--output-format=json",
                "--ioengine=windowsaio",
                "--direct=1",
                "--thread=1",
                "--time_based=1",
                "--runtime=$runtime",
                "--group_reporting=1",
                "--bs=$bs",
                "--iodepth=$iodepth",
                "--rw=$workload",
                "--size=$size"
            )

            if ($workload -eq "randread") {
                $args += "--readonly"
            }

            foreach ($idx in 0..($drives.Count - 1)) {
                $args += "--name=d$idx"
                $args += "--filename=$($drives[$idx])"
                $args += "--numjobs=$threads"
            }

            $jsonText = & $fio @args 2>&1 | Out-String
            $exitCode = $LASTEXITCODE

            if ($exitCode -ne 0) {
                Write-Warning "fio failed for workload=$workload threads=$threads run=$run exitcode=$exitCode"
                $escaped = $jsonText -replace "`r?`n", " " -replace ",", ";"
                "$timestamp,$workload,$threads,$run,0,0,0,0,0,0,0,0,$exitCode" |
                    Add-Content -Encoding ascii $outCsv
                continue
            }

            try {
                $result = $jsonText | ConvertFrom-Json
            } catch {
                Write-Warning "fio JSON parse failed for workload=$workload threads=$threads run=$run"
                "$timestamp,$workload,$threads,$run,0,0,0,0,0,0,0,0,999" |
                    Add-Content -Encoding ascii $outCsv
                continue
            }

            $job = $result.jobs[0]

            $readIops  = if ($null -ne $job.read.iops)        { $job.read.iops } else { 0 }
            $writeIops = if ($null -ne $job.write.iops)       { $job.write.iops } else { 0 }
            $readBw    = if ($null -ne $job.read.bw)          { $job.read.bw } else { 0 }
            $writeBw   = if ($null -ne $job.write.bw)         { $job.write.bw } else { 0 }
            $readClat  = if ($null -ne $job.read.clat_ns.mean){ $job.read.clat_ns.mean } else { 0 }
            $writeClat = if ($null -ne $job.write.clat_ns.mean){ $job.write.clat_ns.mean } else { 0 }
            $readLat   = if ($null -ne $job.read.lat_ns.mean) { $job.read.lat_ns.mean } else { 0 }
            $writeLat  = if ($null -ne $job.write.lat_ns.mean){ $job.write.lat_ns.mean } else { 0 }

            "$timestamp,$workload,$threads,$run,$readIops,$writeIops,$readBw,$writeBw,$readClat,$writeClat,$readLat,$writeLat,$exitCode" |
                Add-Content -Encoding ascii $outCsv
        }
    }
}

Write-Host "Done. Results written to $outCsv"
