. $PSScriptRoot\Invoke-LogServerSimulator.ps1
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

# Run the simulator with the settings in the $settings variable above
# and simultaneously export each row to CSV and display it in Out-GridView
Invoke-LogServerSimulator @settings | Out-GridView
