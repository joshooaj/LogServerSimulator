function Invoke-LogServerSimulator {
    [CmdletBinding()]
    param (
        # Specifies the number of new log messages generated per day
        [Parameter(Mandatory)]
        [ValidateRange(1, [int]::MaxValue)]
        [int]
        $LogsPerDay,

        # Specifies the log retention in days
        [Parameter()]
        [ValidateRange(1, [int]::MaxValue)]
        [int]
        $LogRetention = 30,

        # Specifies the number of days to run the simulation
        [Parameter()]
        [ValidateRange(1, [int]::MaxValue)]
        [int]
        $SimulationDuration,

        # Specifies the configured LogDeleteThreshold value in the VideoOS.LogServer.exe.config file.
        # The value is a percentage represented as an integer between 1 and 100. The default is 20,
        # and the value of 0 has special meaning and should not be used in production unless approved
        # by the log server engineers.
        [Parameter()]
        [ValidateRange(0, 100)]
        [int]
        $LogDeleteThreshold = 20,

        # Specifies the maximum time, in days, between table swap operations. The default value is 7.
        [Parameter()]
        [ValidateRange(1, [int]::MaxValue)]
        [int]
        $ExpiredLogsMaximumLifetimeInDays = 7,

        # Add or subtract a random number of daily log records for a more realistic simulation.
        # Expressed as a percentage value where 0.2 means up to 20% deviation of the daily logs per day
        [Parameter()]
        [ValidateRange(0,1)]
        [double]
        $MaxDeviation = 0
    )
    
    process {
        $active = New-Object system.collections.generic.list[int]
        $inactive = New-Object system.collections.generic.list[int]
        $swap = New-Object system.collections.generic.list[int]
        $lastTableSwap = 1
        $nextTableSwap = $lastTableSwap + $ExpiredLogsMaximumLifetimeInDays
        
        for ($day = 1; $day -le $SimulationDuration; $day++) {
            # Add new logs to the active table
            $logsToCreateToday = $LogsPerDay
            if ($MaxDeviation -ne 0) {
                $deviation = Get-Random -Minimum 0 -Maximum ([int]($LogsPerDay * $MaxDeviation))
                if ($deviation % 2) {
                    $deviation *= -1
                }
                $logsToCreateToday += $deviation
            }
            1..$logsToCreateToday | Foreach-Object {
                $active.Add($day)
            }

            # Calculate the % not expired
            $percentNotExpired = 100
            $countNotExpired = 0
            $countExpired = 0
            $cutoffDay = $day - $LogRetention
            if ($inactive.Count -gt 0) {
                for ($i = 0; $i -lt $inactive.Count; $i++) {
                    if ($inactive[$i] -lt $cutoffDay) {
                        $countExpired++
                    } else {
                        break
                    }
                }
                $countNotExpired = $inactive.Count - $countExpired
                $percentNotExpired = [int]($countNotExpired / $inactive.Count * 100)
            }

            # Simulate table swap if unexpired logs is less than or equal to the threshold or the max days between swaps is reached
            $rowsCopied = 0
            $tablesSwapped = $false
            $tableSwapReason = $null
            if ($percentNotExpired -le $LogDeleteThreshold -or $day - $lastTableSwap -ge $ExpiredLogsMaximumLifetimeInDays) {
                $swap = $inactive | Where-Object { $_ -gt $cutoffDay}
                $rowsCopied = $swap.Count
                $inactive = $active
                $active = New-Object system.collections.generic.list[int]
                $swap | Foreach-Object { $active.Add($_) }
                $lastTableSwap = $day
                $nextTableSwap = $lastTableSwap + $ExpiredLogsMaximumLifetimeInDays
                $tablesSwapped = $true
                if ($percentNotExpired -le $LogDeleteThreshold) {
                    $tableSwapReason = 'LogDeleteThreshold'
                } else {
                    $tableSwapReason = 'ExpiredLogsMaximumLifetimeInDays'
                }
            }

            # Output the daily statistics
            [PSCustomObject]@{
                Day = $day
                ActiveTableSize = $active.Count
                InactiveTableSize = $inactive.Count
                TotalRecords = $active.Count + $inactive.Count
                PercentNotExpired = $percentNotExpired
                TableSwapOccurred = $tablesSwapped
                TableSwapTrigger = $tableSwapReason
                RowsCopied = $rowsCopied
                NextTableSwap = $nextTableSwap
                OldestActiveRecord = if ($active.Count -gt 0) { $day - $active[0] } else { 0 }
                OldestInactiveRecord = if ($inactive.Count -gt 0) { $day - $inactive[0] } else { 0 }
                NewestInactiveRecord = if ($inactive.Count -gt 0) { $day - $inactive[-1] } else { 0 }
            } 
        }
    }
}

<#
    Sample Test Case:
    - 104,150 records per day
    - 365 day retention
    - 5% threshold or 1% threshold
#>

# This defines the settings for the simulation
$settings = @{
    LogsPerDay = 100
    LogRetention = 365

    # Specified in Days.
    SimulationDuration = 365 * 3
    
    # The percentage of records in the inactive table that are NOT expired
    # A table swap is triggered when the percentage of NOT expired records
    # is less than, or equal to this value.
    LogDeleteThreshold = 1

    # The maximum number of days before forcing a table swap even if
    # LogDeleteThreshold is not yet reached.
    ExpiredLogsMaximumLifetimeInDays = 366

    # Adds a random daily deviation for LogsPerDay of up to +-5%
    MaxDeviation = 0.05
}

# Create file name based on settings and remove the previous CSV file if present
$fileName = 'TableSwap-Simulation_{0}-logs-per-day_{1}-days-retention_{2}-threshold_{3}-max-days-between-swaps.csv' -f $settings.LogsPerDay, $settings.LogRetention, $settings.LogDeleteThreshold, $settings.ExpiredLogsMaximumLifetimeInDays
$path = Join-Path $PSScriptRoot $fileName
if (Test-Path $path) {
    Remove-Item -Path $path
}

# Run the simulator with the settings in the $settings variable above
# and simultaneously export each row to CSV and display it in Out-GridView
Invoke-LogServerSimulator @settings | Foreach-Object {
    $_ | Export-Csv -Path $path -NoTypeInformation -Append
    $_
} | Out-GridView
