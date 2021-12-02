function Test-LogServerBehavior {
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
        $ExpiredLogsMaximumLifetimeInDays = 7
    )
    
    process {
        $active = New-Object system.collections.generic.list[int]
        $inactive = New-Object system.collections.generic.list[int]
        $swap = New-Object system.collections.generic.list[int]
        $lastTableSwap = 1
        $nextTableSwap = $lastTableSwap + $ExpiredLogsMaximumLifetimeInDays
        for ($day = 1; $day -le $SimulationDuration; $day++) {
            1..$LogsPerDay | Foreach-Object {
                $active.Add($day)
            }

            $percentNotExpired = 100
            $countNotExpired = 0
            $cutoffDay = $day - $LogRetention
            if ($inactive.Count -gt 0) {
                for ($i = 0; $i -lt $inactive.Count; $i++) {
                    if ($inactive[$i] -ge $cutoffDay) {
                        $countNotExpired++
                    }
                }
                $percentNotExpired = [int]($countNotExpired / $inactive.Count * 100)
            }

            $rowsCopied = 0
            $tablesSwapped = $false
            if ($percentNotExpired -le $LogDeleteThreshold -or $day - $lastTableSwap -ge $ExpiredLogsMaximumLifetimeInDays) {
                $swap = $inactive | Where-Object { $_ -gt $cutoffDay}
                $rowsCopied = $swap.Count
                $inactive = $active
                $active = New-Object system.collections.generic.list[int]
                $swap | Foreach-Object { $active.Add($_) }
                $lastTableSwap = $day
                $nextTableSwap = $lastTableSwap + $ExpiredLogsMaximumLifetimeInDays
            }

            [PSCustomObject]@{
                Day = $day
                ActiveTableSize = $active.Count
                InactiveTableSize = $inactive.Count
                PercentNotExpired = $percentNotExpired
                TableSwapOccurred = $tablesSwapped
                RowsCopied = $rowsCopied
                NextTableSwap = $nextTableSwap
            }            
        }
    }
}

<#
    Assumptions
    •	5% threshold (which we did set)
    •	The weekly procedure is stopped, table swaps only happen at the 5% threshold.
    •	104,150 new records per day (based off of your average rate over the last year)
    •	Expiration policy of 365 days (which is the current policy).
#>

Clear-Host
$settings = @{
    LogsPerDay = 104150
    LogRetention = 365
    SimulationDuration = 365 * 3
    LogDeleteThreshold = 5
    ExpiredLogsMaximumLifetimeInDays = 366
}

$testSettings = @{
    LogsPerDay = 100
    LogRetention = 30
    SimulationDuration = 365
    LogDeleteThreshold = 20
    ExpiredLogsMaximumLifetimeInDays = 7
}

$results = new-object system.collections.generic.list[pscustomobject]
Test-LogServerBehavior @testSettings | Foreach-Object {
    $results.Add($_)
    $_
} | Out-GridView

$settings = $testSettings
$fileName = 'TableSwap-Simulation_{0}-logs-per-day_{1}-days-retention_{2}-threshold_{3}-max-days-between-swaps.csv' -f $settings.LogsPerDay, $settings.LogRetention, $settings.LogDeleteThreshold, $settings.ExpiredLogsMaximumLifetimeInDays
$results | Export-Csv -Path $PSScriptRoot/$fileName -NoTypeInformation -Force

#Test-LogServerBehavior -LogsPerDay 104150 -LogRetention 30 -SimulationDuration 60 | Format-Table