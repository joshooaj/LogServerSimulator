function Invoke-LogServerSimulator {
    <#
    .SYNOPSIS
    Runs a Milestone Log Server simulation to estimate daily SQL table sizes
    and table swap behavior.
    
    .DESCRIPTION
    The Milestone Log Server writes logs to MSSQL under the database name
    "SurveillanceLogServerV2" by default. If your configuration database is not
    named Surveillance, then it will be named "SomethingElseLogServerV2".

    Logs are always written to the MessageInstance_Active table. Periodically,
    the log server will check the MessageInstance_Inactive table to see what
    percentage of records there are not yet expired. If that value is <= 20%,
    controlled by the LogDeleteThreshold setting in VideoOS.LogServer.exe.config,
    a "table swap" operation is initiated. Alternatively, if it has been 7 days
    since the last table swap, the process will be initiated regardless of the
    percentage of "alive" log messages in the MessageInstance_Inactive table.

    There is a special case where the MessageInstance_Inactive table is empty,
    which most commonly happens on a new installation. When the "inactive"
    table is empty, a table swap is always initiated.

    The table swap operation involves the following steps:

    - Create a new table called MessageInstance_0
    - Copy all "alive" records, or records that have not yet expired, from the
      inactive table to the new table
    - Drop the MessageInstance_Inactive table
    - Rename MessageInstance_Active to MessageInstance_Inactive
    - Rename MessageInstance_0 to MessageInstance_Active

    This procedure helps minimize the cost of grooming the logs by dropping
    entire tables instead of deleting individual records from them. However, in
    many cases it can come at the cost of a large hit to the SQL transaction
    log.

    When we copy the not-expired logs out of MessageInstance_Inactive, each of
    those operations are written to the SQL transaction log. The transaction
    log keeps track of every operation in order to provide point-in-time
    database restoration, and roll-back of failed transactions. If the
    transaction log reaches the maximum configured size and either does not
    allow auto-growth, or auto-growth has resulted in a full disk before the
    table swap operation completes, it can result in a Management Server
    failure due to the SQL database becoming inoperable.
    
    This is true even if your recovery mode in SQL is set to "Simple". In
    simple recovery mode, your transaction log does not retain the information
    necessary to reverse or re-run transactions after the transactions
    complete. It does however get used to log every operation performed in a
    single transaction, in order to recover in case the transaction fails
    before it completes. And all of the records copied from the inactive table
    to the new table are performed in a single operation. So if you run out of
    disk space due to transaction log growth during this operation, it can mean
    downtime.

    This simulator is intended to help with simulating different log server
    configurations to see what we should expect to see in production with
    regard to table swap frequency, table sizes, and the number of records
    copied during the table swap operation. You can tune your system's settings
    to minimize the number of rows copied during the table swap operation and
    thus minimize the impact to the SQL transaction log. However, this may come
    at the cost of storing expired log records much longer than configured, or
    potentially risking GDPR violation by storing user audit logs much longer
    than intended.

    Use this tool to understand the interplay of these settings, and measure
    the tradeoffs of database size, overall retention, and the impact of the
    table swap operation in different configurations.

    
    .PARAMETER LogsPerDay
    Specifies the number of logs to add to the MessageInstance_Active table per
    day. Note that this can be varied day to day by using the MaxDeviation
    parameter.
    
    .PARAMETER LogRetention
    Specifies how long the log server retains records in days.
    
    .PARAMETER SimulationDuration
    Specifies how many days to simulate. The default value is 2 * LogRetention.
    
    .PARAMETER LogDeleteThreshold
    Specifies a percentage value between 1 and 100. When the percentage of logs
    in the MessageInstance_Inactive table which are not yet expired is <= this
    value, a table swap operation will be triggered. The default value is 20.
    
    .PARAMETER ExpiredLogsMaximumLifetimeInDays
    Specifies the maximum time, in days, between two table swap operations.
    This option was added to the log server to help ensure we do not retain
    logs longer than appropriate based on GDPR guidelines. The default value is
    7 days.
    
    .PARAMETER MaxDeviation
    Specifies a percentage value between 0 and 1 where 0.05 means 5%. Use this
    parameter to add some "noise" to the number of logs written per day.
    
    For example, in a simple case where you log 100 messages per day, you could
    use a value of 0.05 to add or subtract up to 5 messages per day at random.

    The average log messages per day should still be close to the LogsPerDay
    parameter.

    .PARAMETER IncludeProcessingTime
    Adds a "ProcessingTime" column to each day to understand how much time was
    spent simulating that day. This is purely for diagnostic purposes and to
    inspire future improvements. Hopefully this can be made more efficient so
    that we can accurately simulate the behavior with very large data sets.
    
    .EXAMPLE
    Invoke-LogServerSimulator -LogPerDay 1000 -LogRetention 90 -SimulationDuration 365 | Out-GridView

    Simulates a year of log server operation, producing day-by-day data showing
    the estimated sizes of the active/inactive tables, when table swap
    operations would occur, and how many, if any, records would be copied during
    those operations.
    
    .NOTES
    This tool is experimental and cannot accurately predict real behavior in a
    production environment. In production, you can have wildly different sets
    of data written to the log day over day, and logs are written constantly, not
    just once per day. Because of this, you cannot expect to eliminate the copying
    of records during a table swap by using settings that seem to work with the
    simulator. You can, however, use this as a tool to determine which
    configuration parameters will offer better outcomes.
    #>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [ValidateRange(1, [int]::MaxValue)]
        [int]
        $LogsPerDay,

        [Parameter()]
        [ValidateRange(1, [int]::MaxValue)]
        [int]
        $LogRetention = 30,

        [Parameter()]
        [ValidateRange(1, [int]::MaxValue)]
        [int]
        $SimulationDuration,

        [Parameter()]
        [ValidateRange(0, 100)]
        [int]
        $LogDeleteThreshold = 20,

        [Parameter()]
        [ValidateRange(1, [int]::MaxValue)]
        [int]
        $ExpiredLogsMaximumLifetimeInDays = 7,

        [Parameter()]
        [ValidateRange(0,1)]
        [double]
        $MaxDeviation = 0,

        [Parameter()]
        [switch]
        $IncludeProcessingTime
    )
    
    process {
        if ($SimulationDuration -eq 0) {
            $SimulationDuration = $LogRetention * 2
        }
        $active = New-Object system.collections.generic.list[int]
        $inactive = New-Object system.collections.generic.list[int]
        $swap = New-Object system.collections.generic.list[int]
        $lastTableSwap = 1
        $nextTableSwap = $lastTableSwap + $ExpiredLogsMaximumLifetimeInDays
        
        $stopwatch = [diagnostics.stopwatch]::new()
        for ($day = 1; $day -le $SimulationDuration; $day++) {
            $stopwatch.Restart()
            Write-Verbose "Day $day"
            # Add new logs to the active table
            $logsToCreateToday = $LogsPerDay
            if ($MaxDeviation -ne 0) {
                $deviation = Get-Random -Minimum 0 -Maximum ([int]($LogsPerDay * $MaxDeviation) + 1)
                if ($deviation % 2) {
                    $deviation *= -1
                }
                $logsToCreateToday += $deviation
            }
            
            1..$logsToCreateToday | Foreach-Object {
                $active.Add($day)
            }
            Write-Verbose "  Added $logsToCreateToday records to active table for a total of $($active.Count) records."

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
                $percentNotExpired = [int]([math]::Ceiling($countNotExpired / $inactive.Count * 100))
            }
            Write-Verbose "  Records older than $cutoffDay are expired."
            Write-Verbose "  Expired records in inactive: $countExpired of $($inactive.Count)"
            Write-Verbose "  Active ($($active.Count)): $([string]::Join(', ', $active))"
            Write-Verbose "  Inactive ($($inactive.Count)): $([string]::Join(', ', $inactive))"

            # Simulate table swap if unexpired logs is less than or equal to the threshold or the max days between swaps is reached
            $rowsCopied = 0
            $tablesSwapped = $false
            $tableSwapReason = $null
            $swapCount = 0
            if ($day -eq 1 -or $percentNotExpired -le $LogDeleteThreshold -or $day - $lastTableSwap -ge $ExpiredLogsMaximumLifetimeInDays) {
                if ($day -eq 1) {
                    $tableSwapReason = 'Day1'
                } elseif ($percentNotExpired -le $LogDeleteThreshold) {
                    $tableSwapReason = 'LogDeleteThreshold'
                } else {
                    $tableSwapReason = 'ExpiredLogsMaximumLifetimeInDays'
                }
                
                $swapCount = if ($tableSwapReason -eq 'ExpiredLogsMaximumLifetimeInDays') { 2 } else { 1 }
                for ($i = 0; $i -lt $swapCount; $i++) {
                    $swap = New-Object System.Collections.Generic.List[int]
                    foreach ($row in $inactive) {
                        if ($row -gt $cutoffDay) {
                            $swap.Add($row)
                        }
                    }
                    $rowsCopied += $swap.Count
                    $inactive = $active
                    $active = $swap
                }

                $lastTableSwap = $day
                $nextTableSwap = $lastTableSwap + $ExpiredLogsMaximumLifetimeInDays
                $tablesSwapped = $true
                Write-Verbose "  Table swap copied $rowsCopied rows in $swapCount swap operations, and inactive now has $($inactive.Count) rows moved from active. Reason: $tableSwapReason"
            }

            # Output the daily statistics
            $result = [PSCustomObject]@{
                Day = $day
                ActiveTableSize = $active.Count
                InactiveTableSize = $inactive.Count
                TotalRecords = $active.Count + $inactive.Count
                PercentNotExpired = $percentNotExpired
                TableSwapOccurred = $tablesSwapped
                TableSwapCount = $swapCount
                TableSwapTrigger = $tableSwapReason
                RowsCopied = $rowsCopied
                NextTableSwapOnOrBefore = $nextTableSwap
                OldestActiveRecord = if ($active.Count -gt 0) { $day - $active[0] } else { 0 }
                OldestInactiveRecord = if ($inactive.Count -gt 0) { $day - $inactive[0] } else { 0 }
                NewestInactiveRecord = if ($inactive.Count -gt 0) { $day - $inactive[-1] } else { 0 }
            }

            if ($IncludeProcessingTime) {
                $result | Add-Member -MemberType NoteProperty -Name ProcessingTime -Value $stopwatch.ElapsedMilliseconds
            }

            Write-Output $result
        }
    }
}