BeforeAll {
    . $PSScriptRoot\..\Invoke-LogServerSimulator.ps1
}

Context 'New simulations match previous trusted data sets' {
    It '90-day simulation with default settings' {
        $expectedResults = Import-Clixml "$PSScriptRoot\test-data-30-days-default-settings.xml"
        $actualResults = Invoke-LogServerSimulator -LogsPerDay 10 -LogRetention 30 -SimulationDuration 90 -LogDeleteThreshold 20 -ExpiredLogsMaximumLifetimeInDays 7
        $expectedResults.Count | Should -BeGreaterThan 0
        $actualResults.Count | Should -BeGreaterThan 0
        $properties = $expectedResults | Get-Member -MemberType NoteProperty | Select-Object -ExpandProperty Name
        for ($i = 0; $i -lt $expectedResults.Count; $i++) {
            foreach ($prop in $properties) {
                $actualResults[$i].$prop | Should -Be $expectedResults[$i].$prop
            }
        }
    }

    It '1-year simulation with custom settings' {
        $expectedResults = Import-Clixml "$PSScriptRoot\test-data-365-days-custom-settings.xml"
        $actualResults = Invoke-LogServerSimulator -LogsPerDay 10 -LogRetention 365 -SimulationDuration (365*3) -LogDeleteThreshold 1 -ExpiredLogsMaximumLifetimeInDays 366
        $expectedResults.Count | Should -BeGreaterThan 0
        $actualResults.Count | Should -BeGreaterThan 0
        $properties = $expectedResults | Get-Member -MemberType NoteProperty | Select-Object -ExpandProperty Name
        for ($i = 0; $i -lt $expectedResults.Count; $i++) {
            foreach ($prop in $properties) {
                $actualResults[$i].$prop | Should -Be $expectedResults[$i].$prop
            }
        }
    }
}
