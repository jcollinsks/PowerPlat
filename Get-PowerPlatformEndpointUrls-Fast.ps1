<#
.SYNOPSIS
    High-performance version for large-scale Power Platform endpoint discovery.
    Optimized for 1000+ environments and 100,000+ resources.

.PARAMETER MaxParallelEnvironments
    Number of environments to process in parallel. Default 10.

.PARAMETER MaxParallelFlows
    Number of flows to query concurrently per environment. Default 50.

.PARAMETER OutputPath
    Required. CSV file to write results incrementally.

.PARAMETER SkipFlowDefinitions
    Skip fetching individual flow definitions (much faster, but less detail).

.EXAMPLE
    .\Get-PowerPlatformEndpointUrls-Fast.ps1 -OutputPath ".\endpoints.csv" -MaxParallelEnvironments 20
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$OutputPath,

    [Parameter(Mandatory = $false)]
    [int]$MaxParallelEnvironments = 10,

    [Parameter(Mandatory = $false)]
    [int]$MaxParallelFlows = 50,

    [Parameter(Mandatory = $false)]
    [switch]$SkipFlowDefinitions,

    [Parameter(Mandatory = $false)]
    [string]$EnvironmentFilter = "*"
)

# Requires PowerShell 7+ for parallel processing
if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Warning "PowerShell 7+ recommended for parallel processing. Current version: $($PSVersionTable.PSVersion)"
    Write-Warning "Install from: https://github.com/PowerShell/PowerShell/releases"
    Write-Warning "Falling back to sequential processing (much slower)..."
    $MaxParallelEnvironments = 1
    $MaxParallelFlows = 1
}

#region Setup

$script:TotalEndpoints = 0
$script:TotalEnvironments = 0
$script:ProcessedEnvironments = 0
$script:StartTime = Get-Date
$script:OutputLock = [System.Threading.Mutex]::new($false, "EndpointOutputLock")

# Initialize CSV with headers
$headers = "EnvironmentName,EnvironmentId,ResourceType,ResourceName,ResourceId,ConnectorName,EndpointUrl,UrlLocation,EndpointType,Owner,State,LastModifiedTime"
$headers | Out-File -FilePath $OutputPath -Encoding UTF8

function Write-ResultToCsv {
    param([PSCustomObject]$Result)

    $script:OutputLock.WaitOne() | Out-Null
    try {
        $line = '"{0}","{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}","{9}","{10}","{11}"' -f `
            ($Result.EnvironmentName -replace '"', '""'),
            ($Result.EnvironmentId -replace '"', '""'),
            ($Result.ResourceType -replace '"', '""'),
            ($Result.ResourceName -replace '"', '""'),
            ($Result.ResourceId -replace '"', '""'),
            ($Result.ConnectorName -replace '"', '""'),
            ($Result.EndpointUrl -replace '"', '""'),
            ($Result.UrlLocation -replace '"', '""'),
            ($Result.EndpointType -replace '"', '""'),
            ($Result.Owner -replace '"', '""'),
            ($Result.State -replace '"', '""'),
            ($Result.LastModifiedTime -replace '"', '""')

        $line | Out-File -FilePath $OutputPath -Encoding UTF8 -Append
        $script:TotalEndpoints++
    }
    finally {
        $script:OutputLock.ReleaseMutex()
    }
}

function Get-FlowApiToken {
    try {
        if (Get-Module -ListAvailable -Name Az.Accounts) {
            Import-Module Az.Accounts -ErrorAction SilentlyContinue
            $context = Get-AzContext -ErrorAction SilentlyContinue
            if (-not $context) {
                Connect-AzAccount -ErrorAction Stop | Out-Null
            }
            $token = Get-AzAccessToken -ResourceUrl "https://service.flow.microsoft.com/" -ErrorAction SilentlyContinue
            if ($token) { return $token.Token }
        }
    } catch { }
    return $null
}

function Test-IsDataEndpoint {
    param([string]$Url)

    if ([string]::IsNullOrWhiteSpace($Url)) { return $false }

    # Quick exclusion patterns
    if ($Url -match '\.(png|jpg|jpeg|gif|svg|ico)$') { return $false }
    if ($Url -match 'connectoricons|officialicons|/icon|azureedge\.net|msecnd\.net') { return $false }
    if ($Url -match 'login\.microsoftonline|api\.powerapps\.com|api\.flow\.microsoft\.com') { return $false }
    if ($Url -match 'make\.powerapps|make\.powerautomate|conn-.*\.azurefd\.net') { return $false }

    # Include patterns
    if ($Url -match '\.sharepoint\.com|\.dynamics\.com|\.crm[0-9]*\.|\.database\.windows\.net') { return $true }
    if ($Url -match 'graph\.microsoft\.com|\.vault\.azure\.net|\.azurewebsites\.net') { return $true }
    if ($Url -match '^https?://[a-zA-Z0-9][-a-zA-Z0-9]*\.[a-zA-Z]{2,}') { return $true }

    return $false
}

function Get-EndpointType {
    param([string]$Url)
    if ($Url -match '\.sharepoint\.com') { return "SharePoint" }
    if ($Url -match '\.dynamics\.com|\.crm[0-9]*\.') { return "Dataverse" }
    if ($Url -match '\.database\.windows\.net') { return "SQL Server" }
    if ($Url -match 'graph\.microsoft\.com') { return "Microsoft Graph" }
    if ($Url -match '\.vault\.azure\.net') { return "Azure Key Vault" }
    return "Custom/HTTP"
}

function Get-EnvironmentInfo {
    param([object]$Environment)

    $envName = $Environment.EnvironmentName
    if (-not $envName) { $envName = $Environment.Name }
    if (-not $envName -and $Environment.Internal) { $envName = $Environment.Internal.name }

    $envDisplayName = $Environment.DisplayName
    if (-not $envDisplayName -and $Environment.Internal -and $Environment.Internal.properties) {
        $envDisplayName = $Environment.Internal.properties.displayName
    }
    if (-not $envDisplayName) { $envDisplayName = $envName }

    return @{
        Name = $envName
        DisplayName = $envDisplayName
    }
}

#endregion

#region Fast Processors

function Process-ConnectionsFast {
    param(
        [string]$EnvName,
        [string]$EnvDisplayName
    )

    $count = 0
    try {
        $connections = Get-AdminPowerAppConnection -EnvironmentName $EnvName -ErrorAction SilentlyContinue

        foreach ($conn in $connections) {
            $connParams = $null
            if ($conn.Internal -and $conn.Internal.properties) {
                $connParams = $conn.Internal.properties.connectionParameters
            }

            if (-not $connParams) { continue }

            # Quick URL extraction from known parameter names
            $urlParams = @('server', 'siteUrl', 'baseResourceUrl', 'environmentUrl', 'url', 'host', 'dataset')

            foreach ($paramName in $urlParams) {
                $value = $connParams.$paramName
                if ($value -and $value -is [string] -and (Test-IsDataEndpoint $value)) {
                    Write-ResultToCsv ([PSCustomObject]@{
                        EnvironmentName  = $EnvDisplayName
                        EnvironmentId    = $EnvName
                        ResourceType     = "Connection"
                        ResourceName     = $conn.DisplayName
                        ResourceId       = $conn.ConnectionId
                        ConnectorName    = ($conn.ConnectorName -replace 'shared_', '' -replace '_', ' ')
                        EndpointUrl      = $value
                        UrlLocation      = "Connection Parameter: $paramName"
                        EndpointType     = (Get-EndpointType $value)
                        Owner            = $conn.CreatedBy.displayName
                        State            = "Active"
                        LastModifiedTime = $conn.LastModifiedTime
                    })
                    $count++
                }
            }
        }
    } catch {
        Write-Verbose "Error processing connections for $EnvName : $_"
    }

    return $count
}

function Process-FlowsFast {
    param(
        [string]$EnvName,
        [string]$EnvDisplayName,
        [string]$Token,
        [int]$MaxConcurrent = 50,
        [bool]$SkipDefinitions = $false
    )

    $count = 0
    try {
        $flows = Get-AdminFlow -EnvironmentName $EnvName -ErrorAction SilentlyContinue
        if (-not $flows) { return 0 }

        $flowList = @($flows)
        $totalFlows = $flowList.Count

        if ($SkipDefinitions -or -not $Token) {
            # Fast mode: just get basic info from flow list
            foreach ($flow in $flowList) {
                # Try to extract URLs from connection references if available
                if ($flow.Internal -and $flow.Internal.properties -and $flow.Internal.properties.connectionReferences) {
                    foreach ($connRef in $flow.Internal.properties.connectionReferences.PSObject.Properties) {
                        $refValue = $connRef.Value
                        if ($refValue.swagger -and $refValue.swagger.host) {
                            $url = "https://$($refValue.swagger.host)"
                            if (Test-IsDataEndpoint $url) {
                                Write-ResultToCsv ([PSCustomObject]@{
                                    EnvironmentName  = $EnvDisplayName
                                    EnvironmentId    = $EnvName
                                    ResourceType     = "Cloud Flow"
                                    ResourceName     = $flow.DisplayName
                                    ResourceId       = $flow.FlowName
                                    ConnectorName    = $refValue.displayName
                                    EndpointUrl      = $url
                                    UrlLocation      = "Connection Reference"
                                    EndpointType     = (Get-EndpointType $url)
                                    Owner            = $flow.Owner.displayName
                                    State            = $flow.Internal.properties.state
                                    LastModifiedTime = $flow.LastModifiedTime
                                })
                                $count++
                            }
                        }
                    }
                }
            }
        }
        else {
            # Full mode: fetch flow definitions concurrently
            $headers = @{
                'Authorization' = "Bearer $Token"
                'Accept' = 'application/json'
            }

            # Process in batches
            $batchSize = $MaxConcurrent
            for ($i = 0; $i -lt $totalFlows; $i += $batchSize) {
                $batch = $flowList[$i..([Math]::Min($i + $batchSize - 1, $totalFlows - 1))]

                # Use runspace pool for concurrent requests
                $runspacePool = [runspacefactory]::CreateRunspacePool(1, $MaxConcurrent)
                $runspacePool.Open()

                $jobs = @()
                foreach ($flow in $batch) {
                    $flowId = $flow.FlowName
                    $flowApiUrl = "https://api.flow.microsoft.com/providers/Microsoft.ProcessSimple/environments/$EnvName/flows/$flowId`?api-version=2016-11-01&`$expand=properties.definition"

                    $powershell = [powershell]::Create().AddScript({
                        param($Url, $Headers)
                        try {
                            Invoke-RestMethod -Uri $Url -Headers $Headers -Method Get -TimeoutSec 30 -ErrorAction SilentlyContinue
                        } catch { $null }
                    }).AddArgument($flowApiUrl).AddArgument($headers)

                    $powershell.RunspacePool = $runspacePool

                    $jobs += @{
                        PowerShell = $powershell
                        Handle = $powershell.BeginInvoke()
                        Flow = $flow
                    }
                }

                # Collect results
                foreach ($job in $jobs) {
                    $flowResponse = $job.PowerShell.EndInvoke($job.Handle)
                    $job.PowerShell.Dispose()

                    if ($flowResponse -and $flowResponse.properties -and $flowResponse.properties.definition) {
                        $definition = $flowResponse.properties.definition
                        $flow = $job.Flow

                        # Extract URLs from triggers and actions
                        $urls = Extract-UrlsFromDefinition -Definition $definition

                        foreach ($urlInfo in $urls) {
                            if (Test-IsDataEndpoint $urlInfo.Url) {
                                Write-ResultToCsv ([PSCustomObject]@{
                                    EnvironmentName  = $EnvDisplayName
                                    EnvironmentId    = $EnvName
                                    ResourceType     = "Cloud Flow"
                                    ResourceName     = $flow.DisplayName
                                    ResourceId       = $flow.FlowName
                                    ConnectorName    = $urlInfo.Connector
                                    EndpointUrl      = $urlInfo.Url
                                    UrlLocation      = $urlInfo.Location
                                    EndpointType     = (Get-EndpointType $urlInfo.Url)
                                    Owner            = $flow.Owner.displayName
                                    State            = $flowResponse.properties.state
                                    LastModifiedTime = $flow.LastModifiedTime
                                })
                                $count++
                            }
                        }
                    }
                }

                $runspacePool.Close()
                $runspacePool.Dispose()
            }
        }
    } catch {
        Write-Verbose "Error processing flows for $EnvName : $_"
    }

    return $count
}

function Extract-UrlsFromDefinition {
    param([object]$Definition)

    $urls = @()

    # Convert to JSON and use regex to find all URLs (much faster than recursive parsing)
    $json = $Definition | ConvertTo-Json -Depth 20 -Compress

    # SharePoint sites
    $matches = [regex]::Matches($json, 'https?://[a-zA-Z0-9-]+\.sharepoint\.com/sites/[a-zA-Z0-9_-]+')
    foreach ($match in $matches) {
        $urls += @{ Url = $match.Value; Location = "Flow Definition"; Connector = "SharePoint" }
    }

    # Dataverse/Dynamics
    $matches = [regex]::Matches($json, 'https?://[a-zA-Z0-9-]+\.crm[0-9]*\.dynamics\.com')
    foreach ($match in $matches) {
        $urls += @{ Url = $match.Value; Location = "Flow Definition"; Connector = "Dataverse" }
    }

    # SQL Servers
    $matches = [regex]::Matches($json, '[a-zA-Z0-9-]+\.database\.windows\.net')
    foreach ($match in $matches) {
        $urls += @{ Url = $match.Value; Location = "Flow Definition"; Connector = "SQL Server" }
    }

    # Generic HTTPS URLs (excluding system URLs)
    $matches = [regex]::Matches($json, 'https://[a-zA-Z0-9-]+\.[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}[^"''\\s]*')
    foreach ($match in $matches) {
        $url = $match.Value -replace '["\},\]]+$', ''
        if ((Test-IsDataEndpoint $url) -and -not ($urls | Where-Object { $_.Url -eq $url })) {
            $urls += @{ Url = $url; Location = "Flow Definition"; Connector = "HTTP" }
        }
    }

    # URL-encoded SharePoint sites
    if ($json -match 'sharepoint\.com%2F') {
        $decoded = [System.Uri]::UnescapeDataString($json)
        $matches = [regex]::Matches($decoded, 'https?://[a-zA-Z0-9-]+\.sharepoint\.com/sites/[a-zA-Z0-9_-]+')
        foreach ($match in $matches) {
            if (-not ($urls | Where-Object { $_.Url -eq $match.Value })) {
                $urls += @{ Url = $match.Value; Location = "Flow Definition (Encoded)"; Connector = "SharePoint" }
            }
        }
    }

    return $urls | Sort-Object -Property Url -Unique
}

function Process-AppsFast {
    param(
        [string]$EnvName,
        [string]$EnvDisplayName
    )

    $count = 0
    try {
        $apps = Get-AdminPowerApp -EnvironmentName $EnvName -ErrorAction SilentlyContinue

        foreach ($app in $apps) {
            if ($app.Internal -and $app.Internal.properties -and $app.Internal.properties.connectionReferences) {
                foreach ($connRef in $app.Internal.properties.connectionReferences.PSObject.Properties) {
                    $refValue = $connRef.Value

                    # Check for datasets (SharePoint sites, etc.)
                    if ($refValue.datasets) {
                        foreach ($ds in $refValue.datasets.PSObject.Properties) {
                            $url = $ds.Name
                            if (Test-IsDataEndpoint $url) {
                                Write-ResultToCsv ([PSCustomObject]@{
                                    EnvironmentName  = $EnvDisplayName
                                    EnvironmentId    = $EnvName
                                    ResourceType     = "Canvas App"
                                    ResourceName     = $app.DisplayName
                                    ResourceId       = $app.AppName
                                    ConnectorName    = $refValue.displayName
                                    EndpointUrl      = $url
                                    UrlLocation      = "Data Source"
                                    EndpointType     = (Get-EndpointType $url)
                                    Owner            = $app.Owner.displayName
                                    State            = "Published"
                                    LastModifiedTime = $app.LastModifiedTime
                                })
                                $count++
                            }
                        }
                    }
                }
            }
        }
    } catch {
        Write-Verbose "Error processing apps for $EnvName : $_"
    }

    return $count
}

#endregion

#region Main

Write-Host @"

╔═══════════════════════════════════════════════════════════════════════╗
║     Power Platform Endpoint Discovery - HIGH PERFORMANCE MODE         ║
║     Optimized for large-scale environments (1000+ environments)       ║
╚═══════════════════════════════════════════════════════════════════════╝

"@ -ForegroundColor Cyan

Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  Output file: $OutputPath" -ForegroundColor Gray
Write-Host "  Parallel environments: $MaxParallelEnvironments" -ForegroundColor Gray
Write-Host "  Parallel flows per env: $MaxParallelFlows" -ForegroundColor Gray
Write-Host "  Skip flow definitions: $SkipFlowDefinitions" -ForegroundColor Gray
Write-Host ""

# Install/import modules
$modules = @('Microsoft.PowerApps.Administration.PowerShell', 'Microsoft.PowerApps.PowerShell')
foreach ($module in $modules) {
    if (-not (Get-Module -ListAvailable -Name $module)) {
        Write-Host "Installing $module..." -ForegroundColor Yellow
        Install-Module -Name $module -Force -AllowClobber -Scope CurrentUser
    }
    Import-Module $module -ErrorAction SilentlyContinue
}

# Connect
Write-Host "Connecting to Power Platform..." -ForegroundColor Yellow
try {
    $test = Get-AdminPowerAppEnvironment -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $test) {
        Add-PowerAppsAccount
    }
} catch {
    Add-PowerAppsAccount
}

# Get Flow API token
Write-Host "Getting Flow API token..." -ForegroundColor Yellow
$flowApiToken = Get-FlowApiToken
if ($flowApiToken) {
    Write-Host "  Flow API access: OK" -ForegroundColor Green
} else {
    Write-Host "  Flow API access: Not available (will use basic flow info only)" -ForegroundColor Yellow
}

# Get environments
Write-Host "Fetching environments..." -ForegroundColor Yellow
$environments = Get-AdminPowerAppEnvironment | Where-Object { $_.DisplayName -like $EnvironmentFilter }
$script:TotalEnvironments = $environments.Count
Write-Host "  Found $($script:TotalEnvironments) environments" -ForegroundColor Green
Write-Host ""

# Process environments
Write-Host "Processing environments..." -ForegroundColor Yellow
Write-Host "  Progress will be shown below. Results are written to CSV incrementally." -ForegroundColor Gray
Write-Host ""

if ($PSVersionTable.PSVersion.Major -ge 7 -and $MaxParallelEnvironments -gt 1) {
    # PowerShell 7+ parallel processing
    $environments | ForEach-Object -ThrottleLimit $MaxParallelEnvironments -Parallel {
        $env = $_
        $OutputPath = $using:OutputPath
        $flowApiToken = $using:flowApiToken
        $MaxParallelFlows = $using:MaxParallelFlows
        $SkipFlowDefinitions = $using:SkipFlowDefinitions

        # Import functions (need to redefine in parallel scope)
        ${function:Get-EnvironmentInfo} = $using:function:Get-EnvironmentInfo
        ${function:Test-IsDataEndpoint} = $using:function:Test-IsDataEndpoint
        ${function:Get-EndpointType} = $using:function:Get-EndpointType
        ${function:Write-ResultToCsv} = $using:function:Write-ResultToCsv
        ${function:Process-ConnectionsFast} = $using:function:Process-ConnectionsFast
        ${function:Process-FlowsFast} = $using:function:Process-FlowsFast
        ${function:Process-AppsFast} = $using:function:Process-AppsFast
        ${function:Extract-UrlsFromDefinition} = $using:function:Extract-UrlsFromDefinition

        $envInfo = Get-EnvironmentInfo -Environment $env
        $envName = $envInfo.Name
        $envDisplayName = $envInfo.DisplayName

        Write-Host "  [$envDisplayName] Starting..." -ForegroundColor Cyan

        $connCount = Process-ConnectionsFast -EnvName $envName -EnvDisplayName $envDisplayName
        $flowCount = Process-FlowsFast -EnvName $envName -EnvDisplayName $envDisplayName -Token $flowApiToken -MaxConcurrent $MaxParallelFlows -SkipDefinitions $SkipFlowDefinitions
        $appCount = Process-AppsFast -EnvName $envName -EnvDisplayName $envDisplayName

        $total = $connCount + $flowCount + $appCount
        Write-Host "  [$envDisplayName] Done: $total endpoints (Conn:$connCount Flow:$flowCount App:$appCount)" -ForegroundColor Green
    }
}
else {
    # Sequential fallback for PS5
    $i = 0
    foreach ($env in $environments) {
        $i++
        $envInfo = Get-EnvironmentInfo -Environment $env
        $envName = $envInfo.Name
        $envDisplayName = $envInfo.DisplayName

        Write-Host "  [$i/$($script:TotalEnvironments)] $envDisplayName" -ForegroundColor Cyan

        $connCount = Process-ConnectionsFast -EnvName $envName -EnvDisplayName $envDisplayName
        $flowCount = Process-FlowsFast -EnvName $envName -EnvDisplayName $envDisplayName -Token $flowApiToken -MaxConcurrent 1 -SkipDefinitions $SkipFlowDefinitions
        $appCount = Process-AppsFast -EnvName $envName -EnvDisplayName $envDisplayName

        $total = $connCount + $flowCount + $appCount
        Write-Host "    Found $total endpoints" -ForegroundColor Gray
    }
}

# Summary
$duration = (Get-Date) - $script:StartTime
Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "COMPLETE" -ForegroundColor Green
Write-Host "  Environments processed: $($script:TotalEnvironments)" -ForegroundColor White
Write-Host "  Total endpoints found: $($script:TotalEndpoints)" -ForegroundColor White
Write-Host "  Duration: $($duration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  Output file: $OutputPath" -ForegroundColor White
Write-Host "═══════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan

#endregion
