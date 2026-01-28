<#
.SYNOPSIS
    Extracts the actual endpoint URLs from Power Platform connections, flows, apps, and Copilot Studio agents.

.DESCRIPTION
    This script queries the actual connection instances to find the real endpoint URLs -
    the specific SharePoint sites, SQL servers, Dataverse environments, HTTP endpoints, etc.

    Key difference from other approaches: This script queries the CONNECTION INSTANCES
    themselves, which contain the actual configured URLs, not just connector metadata.

.PARAMETER EnvironmentName
    Specific environment to query. If not specified, queries all environments.

.PARAMETER OutputFormat
    Output format: 'Object', 'CSV', 'JSON'. Default is 'Object'.

.PARAMETER OutputPath
    Path to export results.

.PARAMETER Verbose
    Show detailed progress information.

.EXAMPLE
    .\Get-PowerPlatformEndpointUrls.ps1 -EnvironmentName "Production" -OutputFormat CSV -OutputPath ".\endpoints.csv"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$EnvironmentName,

    [Parameter(Mandatory = $false)]
    [ValidateSet('Object', 'CSV', 'JSON')]
    [string]$OutputFormat = 'Object',

    [Parameter(Mandatory = $false)]
    [string]$OutputPath
)

#region Setup

$script:AllResults = @()

function Install-RequiredModules {
    $modules = @(
        'Microsoft.PowerApps.Administration.PowerShell',
        'Microsoft.PowerApps.PowerShell'
    )

    foreach ($module in $modules) {
        if (-not (Get-Module -ListAvailable -Name $module)) {
            Write-Host "Installing module: $module" -ForegroundColor Yellow
            Install-Module -Name $module -Force -AllowClobber -Scope CurrentUser
        }
        Import-Module $module -ErrorAction SilentlyContinue
    }
}

function Connect-PowerPlatform {
    try {
        $test = Get-AdminPowerAppEnvironment -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($test) {
            Write-Host "Already connected to Power Platform." -ForegroundColor Green
            return $true
        }
    } catch { }

    Write-Host "Connecting to Power Platform..." -ForegroundColor Yellow
    try {
        Add-PowerAppsAccount
        Write-Host "Connected successfully." -ForegroundColor Green
        return $true
    } catch {
        Write-Error "Failed to connect: $_"
        return $false
    }
}

function Get-DataverseToken {
    param([string]$EnvironmentUrl)

    try {
        if (Get-Module -ListAvailable -Name Az.Accounts) {
            Import-Module Az.Accounts -ErrorAction SilentlyContinue
            $context = Get-AzContext -ErrorAction SilentlyContinue
            if (-not $context) {
                Connect-AzAccount -ErrorAction SilentlyContinue
            }
            $token = Get-AzAccessToken -ResourceUrl $EnvironmentUrl -ErrorAction SilentlyContinue
            if ($token) { return $token.Token }
        }
    } catch {
        Write-Verbose "Could not get Dataverse token: $_"
    }
    return $null
}

#endregion

#region Connection Analysis

function Get-ConnectionEndpoints {
    <#
    .SYNOPSIS
        Gets the actual endpoint URLs from connection instances.
    #>
    param([object]$Environment)

    $results = @()
    $envName = $Environment.EnvironmentName
    $envDisplayName = $Environment.DisplayName

    Write-Host "  Querying Connection Instances..." -ForegroundColor Cyan

    try {
        # Get all connections in this environment
        $connections = Get-AdminPowerAppConnection -EnvironmentName $envName -ErrorAction Stop

        foreach ($conn in $connections) {
            $connectorName = $conn.ConnectorName
            $connId = $conn.ConnectionId
            $displayName = $conn.DisplayName
            $statuses = $conn.Statuses

            # Skip if no connection parameters
            $connParams = $null
            if ($conn.Internal -and $conn.Internal.properties) {
                $connParams = $conn.Internal.properties.connectionParameters

                # Also check connectionParametersSet
                if (-not $connParams -and $conn.Internal.properties.connectionParametersSet) {
                    $connParams = $conn.Internal.properties.connectionParametersSet.values
                }
            }

            # Also try the direct ConnectionParameters property
            if (-not $connParams -and $conn.ConnectionParameters) {
                $connParams = $conn.ConnectionParameters
            }

            $endpoints = @()

            if ($connParams) {
                # Extract URL-related parameters
                $endpoints += Extract-EndpointsFromParameters -Parameters $connParams -ConnectorName $connectorName
            }

            # Also check the statuses for connection info
            if ($statuses) {
                foreach ($status in $statuses) {
                    if ($status.target) {
                        $endpoints += [PSCustomObject]@{
                            Url = $status.target
                            ParameterName = "Status Target"
                            ConnectorType = $connectorName
                        }
                    }
                }
            }

            # Add results
            foreach ($endpoint in $endpoints) {
                if ($endpoint.Url -and (Test-IsDataEndpoint $endpoint.Url)) {
                    $results += [PSCustomObject]@{
                        EnvironmentName  = $envDisplayName
                        EnvironmentId    = $envName
                        ResourceType     = "Connection"
                        ResourceName     = $displayName
                        ResourceId       = $connId
                        ConnectorName    = (Get-FriendlyConnectorName $connectorName)
                        ConnectorId      = $connectorName
                        EndpointUrl      = $endpoint.Url
                        ParameterName    = $endpoint.ParameterName
                        EndpointType     = (Get-EndpointType $endpoint.Url)
                        CreatedTime      = $conn.CreatedTime
                        LastModifiedTime = $conn.LastModifiedTime
                        CreatedBy        = $conn.CreatedBy.displayName
                    }
                }
            }
        }

        Write-Host "    Found $($results.Count) endpoint URLs in Connections" -ForegroundColor Gray
    }
    catch {
        Write-Warning "Error querying connections: $_"
    }

    return $results
}

function Extract-EndpointsFromParameters {
    <#
    .SYNOPSIS
        Extracts endpoint URLs from connection parameters.
    #>
    param(
        [object]$Parameters,
        [string]$ConnectorName
    )

    $endpoints = @()

    if ($null -eq $Parameters) { return $endpoints }

    # Known parameter names that contain URLs/endpoints
    $urlParamNames = @(
        # SharePoint
        'siteUrl', 'site', 'webUrl', 'dataset',
        # SQL
        'server', 'database', 'sqlServer',
        # Dataverse/Dynamics
        'environmentUrl', 'organization', 'org', 'instance', 'crmInstance',
        'organizationUrl', 'environmentName',
        # Generic
        'url', 'uri', 'host', 'hostname', 'baseUrl', 'endpoint', 'serviceUrl',
        'apiUrl', 'serverUrl', 'address', 'gateway', 'tenant',
        # Azure
        'accountName', 'storageAccount', 'vaultName', 'keyVaultUrl',
        'blobEndpoint', 'tableEndpoint', 'queueEndpoint',
        # Custom
        'authUrl', 'tokenUrl', 'resourceUrl'
    )

    # Recursively search for URL parameters
    $foundParams = Find-ParametersRecursive -Object $Parameters -ParamNames $urlParamNames -Path ""

    foreach ($param in $foundParams) {
        $value = $param.Value

        # Handle different value types
        if ($value -is [string] -and $value.Length -gt 0) {
            # Check if it looks like a URL or server name
            if ($value -match '^https?://' -or
                $value -match '\.sharepoint\.com' -or
                $value -match '\.dynamics\.com' -or
                $value -match '\.database\.windows\.net' -or
                $value -match '\.blob\.core\.windows\.net' -or
                $value -match '\.crm[0-9]*\.' -or
                $value -match '\.microsoftonline\.com' -or
                $value -match '\.[a-z]+\.[a-z]{2,}' -or
                $param.Name -in @('server', 'database', 'accountName', 'vaultName')) {

                $endpoints += [PSCustomObject]@{
                    Url = $value
                    ParameterName = $param.Name
                    ConnectorType = $ConnectorName
                }
            }
        }
    }

    return $endpoints
}

function Find-ParametersRecursive {
    param(
        [object]$Object,
        [string[]]$ParamNames,
        [string]$Path
    )

    $found = @()

    if ($null -eq $Object) { return $found }

    if ($Object -is [System.Management.Automation.PSCustomObject] -or $Object -is [hashtable]) {
        $properties = if ($Object -is [hashtable]) { $Object.Keys } else { $Object.PSObject.Properties.Name }

        foreach ($propName in $properties) {
            $propValue = if ($Object -is [hashtable]) { $Object[$propName] } else { $Object.$propName }
            $currentPath = if ($Path) { "$Path.$propName" } else { $propName }

            # Check if this property name matches our URL parameter names
            $isUrlParam = $false
            foreach ($paramName in $ParamNames) {
                if ($propName -like "*$paramName*" -or $propName -eq $paramName) {
                    $isUrlParam = $true
                    break
                }
            }

            if ($isUrlParam -and $propValue) {
                $found += [PSCustomObject]@{
                    Name = $propName
                    Value = $propValue
                    Path = $currentPath
                }
            }

            # Recurse into nested objects
            if ($propValue -is [System.Management.Automation.PSCustomObject] -or $propValue -is [hashtable]) {
                $found += Find-ParametersRecursive -Object $propValue -ParamNames $ParamNames -Path $currentPath
            }
            elseif ($propValue -is [array]) {
                for ($i = 0; $i -lt $propValue.Count; $i++) {
                    $found += Find-ParametersRecursive -Object $propValue[$i] -ParamNames $ParamNames -Path "$currentPath[$i]"
                }
            }
        }
    }

    return $found
}

#endregion

#region Flow Analysis

function Get-FlowApiToken {
    <#
    .SYNOPSIS
        Gets an access token for the Flow API.
    #>
    try {
        if (Get-Module -ListAvailable -Name Az.Accounts) {
            Import-Module Az.Accounts -ErrorAction SilentlyContinue
            $context = Get-AzContext -ErrorAction SilentlyContinue
            if (-not $context) {
                Write-Host "    Connecting to Azure for Flow API access..." -ForegroundColor Yellow
                Connect-AzAccount -ErrorAction Stop | Out-Null
            }
            # Get token for Flow API
            $token = Get-AzAccessToken -ResourceUrl "https://service.flow.microsoft.com/" -ErrorAction SilentlyContinue
            if ($token) { return $token.Token }

            # Fallback to management API
            $token = Get-AzAccessToken -ResourceUrl "https://management.azure.com/" -ErrorAction SilentlyContinue
            if ($token) { return $token.Token }
        }
    } catch {
        Write-Verbose "Could not get Flow API token: $_"
    }
    return $null
}

function Get-FlowEndpoints {
    <#
    .SYNOPSIS
        Extracts endpoint URLs from Cloud Flow definitions using the Flow API.
    #>
    param([object]$Environment)

    $results = @()
    $envName = $Environment.EnvironmentName
    $envDisplayName = $Environment.DisplayName

    Write-Host "  Analyzing Cloud Flows..." -ForegroundColor Cyan

    try {
        # Get list of flows
        $flows = Get-AdminFlow -EnvironmentName $envName -ErrorAction Stop
        $flowCount = ($flows | Measure-Object).Count
        Write-Host "    Found $flowCount flows to analyze..." -ForegroundColor Gray

        # Try to get Flow API token for fetching full definitions
        $flowApiToken = Get-FlowApiToken
        $hasApiAccess = $null -ne $flowApiToken

        if ($hasApiAccess) {
            Write-Host "    Using Flow API to get full definitions..." -ForegroundColor Gray
        }

        $flowsWithDef = 0
        $flowsWithoutDef = 0

        foreach ($flow in $flows) {
            $flowName = $flow.DisplayName
            $flowId = $flow.FlowName
            Write-Verbose "  Processing flow: $flowName"

            $definition = $null
            $props = $null

            # Method 1: Use Flow API to get full definition (most reliable)
            if ($hasApiAccess -and -not $definition) {
                try {
                    $flowApiUrl = "https://api.flow.microsoft.com/providers/Microsoft.ProcessSimple/environments/$envName/flows/$flowId`?api-version=2016-11-01&`$expand=properties.definition"

                    $headers = @{
                        'Authorization' = "Bearer $flowApiToken"
                        'Accept' = 'application/json'
                    }

                    $flowResponse = Invoke-RestMethod -Uri $flowApiUrl -Headers $headers -Method Get -ErrorAction SilentlyContinue

                    if ($flowResponse -and $flowResponse.properties) {
                        $props = $flowResponse.properties
                        $definition = $props.definition
                    }
                } catch {
                    Write-Verbose "    Could not get flow via API: $_"
                }
            }

            # Method 2: Try to get definition from AdminFlow response
            if (-not $definition -and $flow.Internal -and $flow.Internal.properties) {
                $props = $flow.Internal.properties
                $definition = $props.definition
            }

            # Method 3: Try direct properties
            if (-not $definition -and $flow.properties) {
                $props = $flow.properties
                $definition = $props.definition
            }

            # Method 4: Try using Get-Flow (non-admin) which may have more details
            if (-not $definition) {
                try {
                    $flowDetails = Get-Flow -EnvironmentName $envName -FlowName $flowId -ErrorAction SilentlyContinue
                    if ($flowDetails -and $flowDetails.Internal -and $flowDetails.Internal.properties) {
                        $props = $flowDetails.Internal.properties
                        $definition = $props.definition
                    }
                } catch {
                    Write-Verbose "    Could not get flow details via Get-Flow: $_"
                }
            }

            # Method 4: Try to get definition from Internal.definition directly
            if (-not $definition -and $flow.Internal -and $flow.Internal.definition) {
                $definition = $flow.Internal.definition
                $props = $flow.Internal
            }

            if (-not $definition) {
                $flowsWithoutDef++
                Write-Verbose "    No definition found for flow: $flowName"
                continue
            }

            $flowsWithDef++

            # Analyze triggers
            if ($definition.triggers) {
                $triggerEndpoints = Analyze-FlowTriggers -Triggers $definition.triggers -ConnectionRefs $props.connectionReferences
                foreach ($ep in $triggerEndpoints) {
                    if (Test-IsDataEndpoint $ep.Url) {
                        $results += [PSCustomObject]@{
                            EnvironmentName  = $envDisplayName
                            EnvironmentId    = $envName
                            ResourceType     = "Cloud Flow"
                            ResourceName     = $flowName
                            ResourceId       = $flowId
                            ConnectorName    = $ep.Connector
                            EndpointUrl      = $ep.Url
                            UrlLocation      = $ep.Location
                            ActionName       = $ep.ActionName
                            EndpointType     = (Get-EndpointType $ep.Url)
                            Owner            = $flow.Owner.displayName
                            State            = $props.state
                            LastModifiedTime = $flow.LastModifiedTime
                        }
                    }
                }
            }

            # Analyze actions (recursive for nested actions)
            if ($definition.actions) {
                $actionEndpoints = Analyze-FlowActionsDeep -Actions $definition.actions -ConnectionRefs $props.connectionReferences
                foreach ($ep in $actionEndpoints) {
                    if (Test-IsDataEndpoint $ep.Url) {
                        $results += [PSCustomObject]@{
                            EnvironmentName  = $envDisplayName
                            EnvironmentId    = $envName
                            ResourceType     = "Cloud Flow"
                            ResourceName     = $flowName
                            ResourceId       = $flowId
                            ConnectorName    = $ep.Connector
                            EndpointUrl      = $ep.Url
                            UrlLocation      = $ep.Location
                            ActionName       = $ep.ActionName
                            EndpointType     = (Get-EndpointType $ep.Url)
                            Owner            = $flow.Owner.displayName
                            State            = $props.state
                            LastModifiedTime = $flow.LastModifiedTime
                        }
                    }
                }
            }
        }

        Write-Host "    Flows with definitions: $flowsWithDef, without: $flowsWithoutDef" -ForegroundColor Gray
        Write-Host "    Found $($results.Count) endpoint URLs in Cloud Flows" -ForegroundColor Gray
    }
    catch {
        Write-Host "    ERROR analyzing flows: $_" -ForegroundColor Red
        Write-Host "    Error details: $($_.Exception.Message)" -ForegroundColor Red
    }

    if ($results.Count -eq 0) {
        Write-Host "    Note: No flow endpoints found via Admin API. Trying Dataverse query..." -ForegroundColor Yellow

        # Try getting flow definitions from Dataverse workflow table
        $dataverseResults = Get-FlowEndpointsFromDataverse -Environment $Environment
        if ($dataverseResults.Count -gt 0) {
            Write-Host "    Found $($dataverseResults.Count) endpoint URLs via Dataverse" -ForegroundColor Gray
            return $dataverseResults
        }
    }

    return $results
}

function Get-FlowEndpointsFromDataverse {
    <#
    .SYNOPSIS
        Gets flow definitions from Dataverse workflow table (for solution-aware flows).
    #>
    param([object]$Environment)

    $results = @()
    $envName = $Environment.EnvironmentName
    $envDisplayName = $Environment.DisplayName

    try {
        # Get Dataverse URL
        $envDetails = Get-AdminPowerAppEnvironment -EnvironmentName $envName
        $dataverseUrl = $null

        if ($envDetails.Internal -and $envDetails.Internal.properties) {
            $linkedEnv = $envDetails.Internal.properties.linkedEnvironmentMetadata
            if ($linkedEnv -and $linkedEnv.instanceUrl) {
                $dataverseUrl = $linkedEnv.instanceUrl.TrimEnd('/')
            }
        }

        if (-not $dataverseUrl) {
            Write-Verbose "No Dataverse environment linked"
            return $results
        }

        $token = Get-DataverseToken -EnvironmentUrl $dataverseUrl
        if (-not $token) {
            Write-Verbose "Could not get Dataverse token"
            return $results
        }

        $headers = @{
            'Authorization' = "Bearer $token"
            'OData-MaxVersion' = '4.0'
            'OData-Version' = '4.0'
            'Accept' = 'application/json'
        }

        # Query workflow table for cloud flows (category = 5 is Modern Flow)
        $workflowUri = "$dataverseUrl/api/data/v9.2/workflows?" +
            "`$filter=category eq 5&" +
            "`$select=name,workflowid,clientdata,statecode,modifiedon&" +
            "`$expand=ownerid(`$select=fullname)"

        Write-Host "    Querying Dataverse for flow definitions..." -ForegroundColor Gray

        $response = Invoke-RestMethod -Uri $workflowUri -Headers $headers -Method Get -ErrorAction Stop
        $workflows = $response.value

        Write-Host "    Found $($workflows.Count) flows in Dataverse" -ForegroundColor Gray

        foreach ($workflow in $workflows) {
            if (-not $workflow.clientdata) { continue }

            try {
                # The clientdata contains the flow definition JSON
                $flowDef = $workflow.clientdata | ConvertFrom-Json

                # Look for the definition in the clientdata
                $definition = $flowDef.properties.definition
                if (-not $definition) {
                    $definition = $flowDef.definition
                }

                if (-not $definition) { continue }

                $flowName = $workflow.name
                $flowId = $workflow.workflowid
                $connRefs = $flowDef.properties.connectionReferences

                # Analyze triggers
                if ($definition.triggers) {
                    $triggerEndpoints = Analyze-FlowTriggers -Triggers $definition.triggers -ConnectionRefs $connRefs
                    foreach ($ep in $triggerEndpoints) {
                        if (Test-IsDataEndpoint $ep.Url) {
                            $results += [PSCustomObject]@{
                                EnvironmentName  = $envDisplayName
                                EnvironmentId    = $envName
                                ResourceType     = "Cloud Flow (Solution)"
                                ResourceName     = $flowName
                                ResourceId       = $flowId
                                ConnectorName    = $ep.Connector
                                EndpointUrl      = $ep.Url
                                UrlLocation      = $ep.Location
                                ActionName       = $ep.ActionName
                                EndpointType     = (Get-EndpointType $ep.Url)
                                Owner            = $workflow.ownerid.fullname
                                State            = if ($workflow.statecode -eq 1) { "On" } else { "Off" }
                                LastModifiedTime = $workflow.modifiedon
                            }
                        }
                    }
                }

                # Analyze actions
                if ($definition.actions) {
                    $actionEndpoints = Analyze-FlowActionsDeep -Actions $definition.actions -ConnectionRefs $connRefs
                    foreach ($ep in $actionEndpoints) {
                        if (Test-IsDataEndpoint $ep.Url) {
                            $results += [PSCustomObject]@{
                                EnvironmentName  = $envDisplayName
                                EnvironmentId    = $envName
                                ResourceType     = "Cloud Flow (Solution)"
                                ResourceName     = $flowName
                                ResourceId       = $flowId
                                ConnectorName    = $ep.Connector
                                EndpointUrl      = $ep.Url
                                UrlLocation      = $ep.Location
                                ActionName       = $ep.ActionName
                                EndpointType     = (Get-EndpointType $ep.Url)
                                Owner            = $workflow.ownerid.fullname
                                State            = if ($workflow.statecode -eq 1) { "On" } else { "Off" }
                                LastModifiedTime = $workflow.modifiedon
                            }
                        }
                    }
                }
            }
            catch {
                Write-Verbose "Error parsing flow $($workflow.name): $_"
            }
        }
    }
    catch {
        Write-Verbose "Error querying Dataverse workflows: $_"
    }

    return $results
}

function Analyze-FlowTriggers {
    param(
        [object]$Triggers,
        [object]$ConnectionRefs
    )

    $endpoints = @()

    foreach ($trigger in $Triggers.PSObject.Properties) {
        $triggerName = $trigger.Name
        $triggerValue = $trigger.Value
        $inputs = $triggerValue.inputs
        $triggerType = $triggerValue.type

        # Determine connector name
        $connector = "Unknown"
        if ($inputs.host -and $inputs.host.connection -and $inputs.host.connection.name) {
            $connRefName = $inputs.host.connection.name
            $connRefName = $connRefName -replace "@parameters\('([^']+)'\)", '$1'
            $connRefName = $connRefName -replace "\['([^']+)'\]", '$1'
            $connRefName = $connRefName -replace "^\$connections\.", ''

            if ($ConnectionRefs -and $ConnectionRefs.$connRefName) {
                $connector = $ConnectionRefs.$connRefName.displayName
                if (-not $connector) {
                    $connId = $ConnectionRefs.$connRefName.id
                    if ($connId) {
                        $connector = $connId -replace '.*/apis/', '' -replace 'shared_', '' -replace '_', ' '
                    }
                }
            } else {
                $connector = $connRefName -replace 'shared_', '' -replace '_', ' '
            }
        }

        if ($inputs) {
            $triggerEndpoints = Extract-EndpointsFromInputs -Inputs $inputs -ActionName $triggerName -Location "Trigger: $triggerName"
            foreach ($ep in $triggerEndpoints) {
                if (-not $ep.Connector -or $ep.Connector -eq "") {
                    $ep.Connector = $connector
                }
                $endpoints += $ep
            }
        }

        # Also check recurrence/metadata for any URLs
        if ($triggerValue.recurrence -or $triggerValue.metadata) {
            # Some triggers store info here
        }
    }

    return $endpoints
}

function Analyze-FlowActionsDeep {
    param(
        [object]$Actions,
        [object]$ConnectionRefs,
        [string]$ParentPath = ""
    )

    $endpoints = @()

    if ($null -eq $Actions) { return $endpoints }

    foreach ($action in $Actions.PSObject.Properties) {
        $actionName = $action.Name
        $actionValue = $action.Value
        $actionType = $actionValue.type
        $currentPath = if ($ParentPath) { "$ParentPath > $actionName" } else { $actionName }

        # Get inputs
        $inputs = $actionValue.inputs

        if ($inputs) {
            $actionEndpoints = Extract-EndpointsFromInputs -Inputs $inputs -ActionName $actionName -Location "Action: $currentPath"

            # Try to determine connector
            $connector = "Unknown"
            if ($inputs.host -and $inputs.host.connection -and $inputs.host.connection.name) {
                $connRefName = $inputs.host.connection.name
                # Remove expression wrapper if present
                $connRefName = $connRefName -replace "@parameters\('([^']+)'\)", '$1'
                $connRefName = $connRefName -replace "\['([^']+)'\]", '$1'

                if ($ConnectionRefs -and $ConnectionRefs.$connRefName) {
                    $connector = $ConnectionRefs.$connRefName.displayName
                    if (-not $connector) {
                        $connector = $ConnectionRefs.$connRefName.id -replace '.*/apis/', '' -replace '_', ' '
                    }
                } else {
                    $connector = $connRefName -replace '\$connections\.', '' -replace '_', ' '
                }
            }

            if ($actionType -eq 'Http') {
                $connector = "HTTP"
            }

            foreach ($ep in $actionEndpoints) {
                $ep.Connector = $connector
                $endpoints += $ep
            }
        }

        # Recurse into nested structures
        if ($actionValue.actions) {
            $endpoints += Analyze-FlowActionsDeep -Actions $actionValue.actions -ConnectionRefs $ConnectionRefs -ParentPath $currentPath
        }
        if ($actionValue.else -and $actionValue.else.actions) {
            $endpoints += Analyze-FlowActionsDeep -Actions $actionValue.else.actions -ConnectionRefs $ConnectionRefs -ParentPath "$currentPath (else)"
        }
        if ($actionValue.cases) {
            foreach ($case in $actionValue.cases.PSObject.Properties) {
                if ($case.Value.actions) {
                    $endpoints += Analyze-FlowActionsDeep -Actions $case.Value.actions -ConnectionRefs $ConnectionRefs -ParentPath "$currentPath (case: $($case.Name))"
                }
            }
        }
        if ($actionValue.default -and $actionValue.default.actions) {
            $endpoints += Analyze-FlowActionsDeep -Actions $actionValue.default.actions -ConnectionRefs $ConnectionRefs -ParentPath "$currentPath (default)"
        }
    }

    return $endpoints
}

function Extract-EndpointsFromInputs {
    param(
        [object]$Inputs,
        [string]$ActionName,
        [string]$Location
    )

    $endpoints = @()

    if ($null -eq $Inputs) { return $endpoints }

    # Direct URL properties
    $urlProps = @('uri', 'url', 'baseUrl', 'endpoint')
    foreach ($prop in $urlProps) {
        if ($Inputs.$prop -and $Inputs.$prop -is [string]) {
            $url = $Inputs.$prop
            # Skip if it's just an expression
            if ($url -notmatch '^@\{' -and $url -match 'https?://') {
                $endpoints += [PSCustomObject]@{
                    Url = (Clean-Url $url)
                    Location = $Location
                    ActionName = $ActionName
                    Connector = ""
                }
            }
        }
    }

    # Parameters section (SharePoint, SQL, Dataverse, etc.)
    $params = $Inputs.parameters
    if ($params) {
        # SharePoint
        if ($params.dataset) {
            $site = $params.dataset
            if ($site -is [string] -and $site -match 'sharepoint\.com|^https?://') {
                $endpoints += [PSCustomObject]@{
                    Url = (Clean-Url $site)
                    Location = "$Location (SharePoint Site)"
                    ActionName = $ActionName
                    Connector = "SharePoint"
                }
            }
        }
        if ($params.table) {
            # This is the list/library name, add it to context
        }

        # SQL Server
        if ($params.server) {
            $server = $params.server
            if ($server -is [string]) {
                $database = $params.database
                $url = if ($database) { "$server/$database" } else { $server }
                $endpoints += [PSCustomObject]@{
                    Url = $url
                    Location = "$Location (SQL Server)"
                    ActionName = $ActionName
                    Connector = "SQL Server"
                }
            }
        }

        # Dataverse / Common Data Service
        if ($params.organization) {
            $org = $params.organization
            if ($org -is [string] -and $org -match '\.dynamics\.com|\.crm') {
                $endpoints += [PSCustomObject]@{
                    Url = (Clean-Url $org)
                    Location = "$Location (Dataverse)"
                    ActionName = $ActionName
                    Connector = "Dataverse"
                }
            }
        }

        # Generic URL parameters
        $urlParamNames = @('siteUrl', 'webUrl', 'baseUrl', 'serviceUrl', 'environmentUrl', 'host', 'hostname')
        foreach ($paramName in $urlParamNames) {
            if ($params.$paramName -and $params.$paramName -is [string]) {
                $value = $params.$paramName
                if ($value -match '^https?://' -or $value -match '\.(com|net|org|io)') {
                    $endpoints += [PSCustomObject]@{
                        Url = (Clean-Url $value)
                        Location = "$Location ($paramName)"
                        ActionName = $ActionName
                        Connector = ""
                    }
                }
            }
        }
    }

    # Path contains URL-encoded site references for SharePoint
    # Example: /datasets/@{encodeURIComponent(encodeURIComponent('https://site.sharepoint.com/sites/name'))}/tables/...
    # Or: /datasets/https%3A%2F%2Fsite.sharepoint.com%2Fsites%2Fname/tables/...
    if ($Inputs.path -and $Inputs.path -is [string]) {
        $path = $Inputs.path

        # Try to URL-decode the path to find encoded URLs
        $decodedPath = $path
        try {
            # Decode twice (SharePoint URLs are often double-encoded)
            $decodedPath = [System.Web.HttpUtility]::UrlDecode($path)
            $decodedPath = [System.Web.HttpUtility]::UrlDecode($decodedPath)
        } catch {
            # If System.Web is not available, use .NET method
            try {
                $decodedPath = [System.Uri]::UnescapeDataString($path)
                $decodedPath = [System.Uri]::UnescapeDataString($decodedPath)
            } catch { }
        }

        # Look for SharePoint URLs in both original and decoded path
        $pathsToCheck = @($path, $decodedPath) | Select-Object -Unique

        foreach ($p in $pathsToCheck) {
            # Match SharePoint URLs (with full site path)
            if ($p -match "(https?://[a-zA-Z0-9-]+\.sharepoint\.com/sites/[a-zA-Z0-9_-]+)") {
                $endpoints += [PSCustomObject]@{
                    Url = $matches[1]
                    Location = "$Location (SharePoint Site)"
                    ActionName = $ActionName
                    Connector = "SharePoint"
                }
            }
            elseif ($p -match "(https?://[a-zA-Z0-9-]+\.sharepoint\.com[^\s'\""}\]@]*)") {
                $url = $matches[1] -replace '/$', ''
                if ($url -and $url.Length -gt 25) {
                    $endpoints += [PSCustomObject]@{
                        Url = $url
                        Location = "$Location (SharePoint)"
                        ActionName = $ActionName
                        Connector = "SharePoint"
                    }
                }
            }

            # Match Dataverse/Dynamics URLs
            if ($p -match "(https?://[a-zA-Z0-9-]+\.crm[0-9]*\.dynamics\.com)") {
                $endpoints += [PSCustomObject]@{
                    Url = $matches[1]
                    Location = "$Location (Dataverse)"
                    ActionName = $ActionName
                    Connector = "Dataverse"
                }
            }

            # Match other URLs in path
            if ($p -match "(https?://[a-zA-Z0-9-]+\.[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}[^\s'\""}\]@]*)") {
                $url = $matches[1] -replace '/$', ''
                # Avoid duplicates and system URLs
                $isDupe = $endpoints | Where-Object { $_.Url -eq $url }
                if (-not $isDupe -and (Test-IsDataEndpoint $url)) {
                    $endpoints += [PSCustomObject]@{
                        Url = $url
                        Location = "$Location (Path)"
                        ActionName = $ActionName
                        Connector = ""
                    }
                }
            }
        }

        # Also extract from expression patterns like encodeURIComponent('https://...')
        $expressionMatches = [regex]::Matches($path, "encodeURIComponent\(['\""](https?://[^'\""\)]+)['\""]\)")
        foreach ($match in $expressionMatches) {
            $url = $match.Groups[1].Value
            if ($url -and (Test-IsDataEndpoint $url)) {
                $isDupe = $endpoints | Where-Object { $_.Url -eq $url }
                if (-not $isDupe) {
                    $endpoints += [PSCustomObject]@{
                        Url = $url
                        Location = "$Location (Expression)"
                        ActionName = $ActionName
                        Connector = ""
                    }
                }
            }
        }
    }

    return $endpoints
}

function Clean-Url {
    param([string]$Url)

    # Remove trailing expressions and clean up
    $clean = $Url -replace "@\{[^}]+\}.*$", ""
    $clean = $clean -replace "\?.*$", ""  # Remove query strings for grouping
    $clean = $clean.TrimEnd('/', '\', '"', "'")

    return $clean
}

#endregion

#region Canvas App Analysis

function Get-CanvasAppEndpoints {
    <#
    .SYNOPSIS
        Extracts endpoint URLs from Canvas App data sources.
    #>
    param([object]$Environment)

    $results = @()
    $envName = $Environment.EnvironmentName
    $envDisplayName = $Environment.DisplayName

    Write-Host "  Analyzing Canvas Apps..." -ForegroundColor Cyan

    try {
        $apps = Get-AdminPowerApp -EnvironmentName $envName -ErrorAction Stop

        foreach ($app in $apps) {
            $appName = $app.DisplayName
            $appId = $app.AppName
            Write-Verbose "  Processing app: $appName"

            if ($app.Internal -and $app.Internal.properties) {
                $props = $app.Internal.properties

                # Connection References - look for dataset/site info
                if ($props.connectionReferences) {
                    foreach ($connRef in $props.connectionReferences.PSObject.Properties) {
                        $refName = $connRef.Name
                        $refValue = $connRef.Value

                        $connectorId = $refValue.id
                        $connectorDisplayName = $refValue.displayName

                        # Look for datasets (SharePoint sites, etc.)
                        if ($refValue.datasets) {
                            foreach ($ds in $refValue.datasets.PSObject.Properties) {
                                $datasetUrl = $ds.Name
                                if ($datasetUrl -and (Test-IsDataEndpoint $datasetUrl)) {
                                    $results += [PSCustomObject]@{
                                        EnvironmentName  = $envDisplayName
                                        EnvironmentId    = $envName
                                        ResourceType     = "Canvas App"
                                        ResourceName     = $appName
                                        ResourceId       = $appId
                                        ConnectorName    = (Get-FriendlyConnectorName $connectorId)
                                        EndpointUrl      = $datasetUrl
                                        UrlLocation      = "Data Source: $($connectorDisplayName ?? $refName)"
                                        DataSourceName   = $ds.Value
                                        EndpointType     = (Get-EndpointType $datasetUrl)
                                        Owner            = $app.Owner.displayName
                                        LastModifiedTime = $app.LastModifiedTime
                                    }
                                }
                            }
                        }

                        # Look for dataSources with tableDefinition
                        if ($refValue.dataSources) {
                            foreach ($ds in $refValue.dataSources.PSObject.Properties) {
                                $dsValue = $ds.Value
                                $dsName = $ds.Name

                                # SharePoint sites
                                $siteUrl = $dsValue.siteUri ?? $dsValue.tableDefinition?.siteUri ?? $dsValue.datasetUri
                                if ($siteUrl -and (Test-IsDataEndpoint $siteUrl)) {
                                    $tableName = $dsValue.tableName ?? $dsValue.tableDefinition?.tableName ?? $dsValue.tableDisplayName
                                    $results += [PSCustomObject]@{
                                        EnvironmentName  = $envDisplayName
                                        EnvironmentId    = $envName
                                        ResourceType     = "Canvas App"
                                        ResourceName     = $appName
                                        ResourceId       = $appId
                                        ConnectorName    = (Get-FriendlyConnectorName $connectorId)
                                        EndpointUrl      = $siteUrl
                                        UrlLocation      = "Data Source: $dsName"
                                        DataSourceName   = $tableName
                                        EndpointType     = (Get-EndpointType $siteUrl)
                                        Owner            = $app.Owner.displayName
                                        LastModifiedTime = $app.LastModifiedTime
                                    }
                                }
                            }
                        }
                    }
                }

                # Embedded app data sources
                if ($props.embeddedApp -and $props.embeddedApp.dataSources) {
                    foreach ($ds in $props.embeddedApp.dataSources.PSObject.Properties) {
                        $dsValue = $ds.Value
                        $dsName = $ds.Name

                        $endpoints = Extract-AppDataSourceEndpoints -DataSource $dsValue -DataSourceName $dsName

                        foreach ($ep in $endpoints) {
                            if (Test-IsDataEndpoint $ep.Url) {
                                $results += [PSCustomObject]@{
                                    EnvironmentName  = $envDisplayName
                                    EnvironmentId    = $envName
                                    ResourceType     = "Canvas App"
                                    ResourceName     = $appName
                                    ResourceId       = $appId
                                    ConnectorName    = $ep.Connector
                                    EndpointUrl      = $ep.Url
                                    UrlLocation      = "Embedded Data Source: $dsName"
                                    DataSourceName   = $ep.TableName
                                    EndpointType     = (Get-EndpointType $ep.Url)
                                    Owner            = $app.Owner.displayName
                                    LastModifiedTime = $app.LastModifiedTime
                                }
                            }
                        }
                    }
                }
            }
        }

        Write-Host "    Found $($results.Count) endpoint URLs in Canvas Apps" -ForegroundColor Gray
    }
    catch {
        Write-Warning "Error analyzing Canvas Apps: $_"
    }

    return $results
}

function Extract-AppDataSourceEndpoints {
    param(
        [object]$DataSource,
        [string]$DataSourceName
    )

    $endpoints = @()

    if ($null -eq $DataSource) { return $endpoints }

    # SharePoint
    $siteUrl = $DataSource.siteUri ?? $DataSource.siteUrl ?? $DataSource.webUrl ??
               $DataSource.tableDefinition?.siteUri ?? $DataSource.tableDefinition?.siteUrl
    if ($siteUrl) {
        $endpoints += [PSCustomObject]@{
            Url = $siteUrl
            Connector = "SharePoint"
            TableName = $DataSource.tableName ?? $DataSource.tableDefinition?.tableName ?? $DataSourceName
        }
    }

    # SQL Server
    if ($DataSource.server) {
        $server = $DataSource.server
        $database = $DataSource.database
        $url = if ($database) { "$server (Database: $database)" } else { $server }
        $endpoints += [PSCustomObject]@{
            Url = $url
            Connector = "SQL Server"
            TableName = $DataSource.table ?? $DataSourceName
        }
    }

    # Dataverse
    $orgUrl = $DataSource.environmentUrl ?? $DataSource.organizationUrl ?? $DataSource.orgUrl ??
              $DataSource.instanceUrl ?? $DataSource.crmUrl
    if ($orgUrl) {
        $endpoints += [PSCustomObject]@{
            Url = $orgUrl
            Connector = "Dataverse"
            TableName = $DataSource.entityName ?? $DataSource.tableName ?? $DataSourceName
        }
    }

    # Custom API / REST
    $apiUrl = $DataSource.baseUri ?? $DataSource.baseUrl ?? $DataSource.endpoint ?? $DataSource.serviceUrl
    if ($apiUrl -and $apiUrl -notmatch 'blob\.core\.windows\.net.*powerapps') {
        $endpoints += [PSCustomObject]@{
            Url = $apiUrl
            Connector = "Custom/REST"
            TableName = $DataSource.operationId ?? $DataSourceName
        }
    }

    return $endpoints
}

#endregion

#region Copilot Studio Analysis

function Get-CopilotEndpoints {
    param([object]$Environment)

    $results = @()
    $envName = $Environment.EnvironmentName
    $envDisplayName = $Environment.DisplayName

    Write-Host "  Analyzing Copilot Studio Agents..." -ForegroundColor Cyan

    try {
        # Get Dataverse URL
        $envDetails = Get-AdminPowerAppEnvironment -EnvironmentName $envName
        $dataverseUrl = $null

        if ($envDetails.Internal -and $envDetails.Internal.properties) {
            $linkedEnv = $envDetails.Internal.properties.linkedEnvironmentMetadata
            if ($linkedEnv -and $linkedEnv.instanceUrl) {
                $dataverseUrl = $linkedEnv.instanceUrl.TrimEnd('/')
            }
        }

        if (-not $dataverseUrl) {
            Write-Verbose "No Dataverse environment for Copilot queries"
            return $results
        }

        $token = Get-DataverseToken -EnvironmentUrl $dataverseUrl
        if (-not $token) {
            Write-Verbose "Could not get token for Copilot queries"
            return $results
        }

        $headers = @{
            'Authorization' = "Bearer $token"
            'OData-MaxVersion' = '4.0'
            'OData-Version' = '4.0'
            'Accept' = 'application/json'
        }

        # Query bots
        $botsUri = "$dataverseUrl/api/data/v9.2/bots?`$select=name,botid,modifiedon&`$expand=ownerid(`$select=fullname)"
        $botsResponse = Invoke-RestMethod -Uri $botsUri -Headers $headers -Method Get -ErrorAction Stop

        foreach ($bot in $botsResponse.value) {
            # Query components
            $componentsUri = "$dataverseUrl/api/data/v9.2/botcomponents?`$filter=_parentbotid_value eq '$($bot.botid)'&`$select=name,componenttype,content"
            $componentsResponse = Invoke-RestMethod -Uri $componentsUri -Headers $headers -Method Get -ErrorAction SilentlyContinue

            if ($componentsResponse -and $componentsResponse.value) {
                foreach ($component in $componentsResponse.value) {
                    if ($component.content) {
                        try {
                            $content = $component.content | ConvertFrom-Json -ErrorAction SilentlyContinue

                            # Look for HTTP actions, flow calls, etc.
                            $componentEndpoints = Find-CopilotEndpoints -Content $content -ComponentName $component.name

                            foreach ($ep in $componentEndpoints) {
                                if (Test-IsDataEndpoint $ep.Url) {
                                    $results += [PSCustomObject]@{
                                        EnvironmentName  = $envDisplayName
                                        EnvironmentId    = $envName
                                        ResourceType     = "Copilot Studio Agent"
                                        ResourceName     = $bot.name
                                        ResourceId       = $bot.botid
                                        ComponentName    = $component.name
                                        EndpointUrl      = $ep.Url
                                        UrlLocation      = $ep.Location
                                        EndpointType     = (Get-EndpointType $ep.Url)
                                        Owner            = $bot.ownerid.fullname
                                        LastModifiedTime = $bot.modifiedon
                                    }
                                }
                            }
                        }
                        catch { }
                    }
                }
            }
        }

        Write-Host "    Found $($results.Count) endpoint URLs in Copilot Studio" -ForegroundColor Gray
    }
    catch {
        Write-Verbose "Error querying Copilot Studio: $_"
    }

    return $results
}

function Find-CopilotEndpoints {
    param(
        [object]$Content,
        [string]$ComponentName,
        [string]$Path = ""
    )

    $endpoints = @()

    if ($null -eq $Content) { return $endpoints }

    if ($Content -is [string]) {
        # Look for URLs in strings
        $urlMatches = [regex]::Matches($Content, 'https?://[^\s"''<>\]\}]+')
        foreach ($match in $urlMatches) {
            $url = $match.Value.TrimEnd('/', '"', "'", ',', ';', ')')
            if (Test-IsDataEndpoint $url) {
                $endpoints += [PSCustomObject]@{
                    Url = $url
                    Location = "Component: $ComponentName"
                }
            }
        }
    }
    elseif ($Content -is [System.Management.Automation.PSCustomObject] -or $Content -is [hashtable]) {
        # Check for HTTP action properties
        if ($Content.url -and $Content.url -is [string]) {
            $endpoints += [PSCustomObject]@{
                Url = $Content.url
                Location = "HTTP Action in $ComponentName"
            }
        }
        if ($Content.uri -and $Content.uri -is [string]) {
            $endpoints += [PSCustomObject]@{
                Url = $Content.uri
                Location = "HTTP Action in $ComponentName"
            }
        }

        # Recurse
        $properties = if ($Content -is [hashtable]) { $Content.Keys } else { $Content.PSObject.Properties.Name }
        foreach ($prop in $properties) {
            $value = if ($Content -is [hashtable]) { $Content[$prop] } else { $Content.$prop }
            $endpoints += Find-CopilotEndpoints -Content $value -ComponentName $ComponentName -Path "$Path.$prop"
        }
    }
    elseif ($Content -is [array]) {
        foreach ($item in $Content) {
            $endpoints += Find-CopilotEndpoints -Content $item -ComponentName $ComponentName -Path $Path
        }
    }

    return $endpoints
}

#endregion

#region Helpers

function Test-IsDataEndpoint {
    <#
    .SYNOPSIS
        Determines if a URL is an actual data endpoint vs. system/icon URL.
    #>
    param([string]$Url)

    if ([string]::IsNullOrWhiteSpace($Url)) { return $false }

    # Exclude patterns (icons, system URLs, CDN, etc.)
    $excludePatterns = @(
        '\.png$', '\.jpg$', '\.jpeg$', '\.gif$', '\.svg$', '\.ico$',  # Images
        'connectoricons', 'officialicons', '/icon', 'icons/',          # Icon URLs
        'blob\.core\.windows\.net.*(powerapps|icons|images)',          # PowerApps blob storage for icons
        'azureedge\.net', 'msecnd\.net', 'akamaized\.net',            # CDN
        'login\.microsoftonline\.com', 'login\.windows\.net',          # Auth endpoints
        'management\.azure\.com/providers/Microsoft\.PowerApps',       # Management API
        'api\.powerapps\.com', 'api\.flow\.microsoft\.com',            # PowerApps API
        'make\.powerapps\.com', 'make\.powerautomate\.com',            # Maker portals
        'flow\.microsoft\.com/manage',                                  # Flow management
        'tip1\.powerva\.microsoft\.com', 'powerva\.microsoft\.com',    # PVA system
        'directline\.botframework\.com', 'token\.botframework\.com',   # Bot framework
        'schemas\.microsoft\.com', 'schema\.org',                       # Schema URLs
        '/providers/Microsoft\.PowerApps/',                             # Resource provider paths
        'gateway\.prod\.',                                              # Gateway endpoints
        'conn-.*\.azurefd\.net'                                        # Connector frontdoor
    )

    foreach ($pattern in $excludePatterns) {
        if ($Url -match $pattern) {
            return $false
        }
    }

    # Include patterns (actual data endpoints)
    $includePatterns = @(
        '\.sharepoint\.com',           # SharePoint
        '\.dynamics\.com',             # Dynamics/Dataverse
        '\.crm[0-9]*\.',              # Dataverse CRM
        '\.database\.windows\.net',    # Azure SQL
        '\.documents\.azure\.com',     # Cosmos DB
        '\.blob\.core\.windows\.net(?!.*(powerapps|icons))',  # Azure Blob (but not PowerApps icons)
        '\.table\.core\.windows\.net', # Azure Table
        '\.queue\.core\.windows\.net', # Azure Queue
        '\.vault\.azure\.net',         # Key Vault
        '\.servicebus\.windows\.net',  # Service Bus
        '\.azurewebsites\.net',        # App Service
        '\.azure-api\.net',            # API Management
        'graph\.microsoft\.com',       # Graph API
        'api\.'                        # Generic API endpoints
    )

    foreach ($pattern in $includePatterns) {
        if ($Url -match $pattern) {
            return $true
        }
    }

    # If it looks like a custom URL with a domain, include it
    if ($Url -match '^https?://[a-zA-Z0-9][-a-zA-Z0-9]*\.[a-zA-Z]{2,}') {
        return $true
    }

    # Server names (SQL, etc.) without protocol
    if ($Url -match '\.database\.windows\.net' -or $Url -match '\.sql\.') {
        return $true
    }

    return $false
}

function Get-EndpointType {
    param([string]$Url)

    if ($Url -match '\.sharepoint\.com') { return "SharePoint" }
    if ($Url -match '\.dynamics\.com|\.crm[0-9]*\.') { return "Dataverse" }
    if ($Url -match '\.database\.windows\.net|sql') { return "SQL Server" }
    if ($Url -match '\.blob\.core\.windows\.net') { return "Azure Blob Storage" }
    if ($Url -match '\.table\.core\.windows\.net') { return "Azure Table Storage" }
    if ($Url -match '\.vault\.azure\.net') { return "Azure Key Vault" }
    if ($Url -match '\.azurewebsites\.net') { return "Azure App Service" }
    if ($Url -match 'graph\.microsoft\.com') { return "Microsoft Graph" }
    if ($Url -match '\.servicebus\.windows\.net') { return "Azure Service Bus" }
    if ($Url -match '\.azure-api\.net') { return "Azure API Management" }

    return "Custom/HTTP"
}

function Get-FriendlyConnectorName {
    param([string]$ConnectorId)

    if (-not $ConnectorId) { return "Unknown" }

    $name = $ConnectorId -replace '.*/apis/', '' -replace '/.*', ''
    $name = $name -replace 'shared_', '' -replace '_', ' '

    # Title case
    $name = (Get-Culture).TextInfo.ToTitleCase($name.ToLower())

    return $name
}

#endregion

#region Output

function Export-Results {
    param(
        [array]$Results,
        [string]$Format,
        [string]$Path
    )

    # Remove duplicates
    $unique = $Results | Sort-Object EnvironmentId, ResourceType, ResourceName, EndpointUrl -Unique

    switch ($Format) {
        'CSV' {
            if ($Path) {
                $unique | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
                Write-Host "`nResults exported to: $Path" -ForegroundColor Green
            } else {
                $unique | ConvertTo-Csv -NoTypeInformation
            }
        }
        'JSON' {
            $json = $unique | ConvertTo-Json -Depth 10
            if ($Path) {
                $json | Out-File -FilePath $Path -Encoding UTF8
                Write-Host "`nResults exported to: $Path" -ForegroundColor Green
            } else {
                $json
            }
        }
        'Object' {
            $unique
        }
    }
}

function Show-Summary {
    param([array]$Results)

    Write-Host "`n$("=" * 70)" -ForegroundColor Cyan
    Write-Host "ENDPOINT URL DISCOVERY SUMMARY" -ForegroundColor Cyan
    Write-Host ("=" * 70) -ForegroundColor Cyan

    $unique = $Results | Sort-Object EndpointUrl -Unique

    Write-Host "`nTotal Unique Endpoint URLs Found: $($unique.Count)" -ForegroundColor White

    if ($unique.Count -gt 0) {
        Write-Host "`nBy Endpoint Type:" -ForegroundColor Yellow
        $unique | Group-Object EndpointType | Sort-Object Count -Descending | ForEach-Object {
            Write-Host "  $($_.Name): $($_.Count)" -ForegroundColor Gray
        }

        Write-Host "`nBy Resource Type:" -ForegroundColor Yellow
        $Results | Group-Object ResourceType | Sort-Object Count -Descending | ForEach-Object {
            Write-Host "  $($_.Name): $($_.Count)" -ForegroundColor Gray
        }

        Write-Host "`nUnique Endpoints:" -ForegroundColor Yellow
        $unique | Select-Object -ExpandProperty EndpointUrl -Unique | Sort-Object | ForEach-Object {
            Write-Host "  $_" -ForegroundColor Gray
        }
    }

    Write-Host ("=" * 70) -ForegroundColor Cyan
}

#endregion

#region Main

function Main {
    Write-Host @"

╔═══════════════════════════════════════════════════════════════════════╗
║        Power Platform Endpoint URL Discovery Tool v2                  ║
║  Finds the ACTUAL URLs your resources connect to                      ║
╚═══════════════════════════════════════════════════════════════════════╝

"@ -ForegroundColor Cyan

    Install-RequiredModules

    if (-not (Connect-PowerPlatform)) {
        Write-Error "Failed to connect. Exiting."
        return
    }

    # Get environments
    $environments = @()
    if ($EnvironmentName) {
        Write-Host "Querying environment: $EnvironmentName" -ForegroundColor Yellow
        $env = Get-AdminPowerAppEnvironment -EnvironmentName $EnvironmentName -ErrorAction SilentlyContinue
        if (-not $env) {
            $env = Get-AdminPowerAppEnvironment | Where-Object { $_.DisplayName -eq $EnvironmentName } | Select-Object -First 1
        }
        if ($env) { $environments += $env }
        else {
            Write-Error "Environment not found: $EnvironmentName"
            return
        }
    } else {
        Write-Host "Querying all environments..." -ForegroundColor Yellow
        $environments = Get-AdminPowerAppEnvironment
    }

    Write-Host "Found $($environments.Count) environment(s)`n" -ForegroundColor Green

    # Collect results
    $allResults = @()

    foreach ($env in $environments) {
        Write-Host "Processing: $($env.DisplayName)" -ForegroundColor Yellow

        # Get connection instances (this has the actual endpoint URLs!)
        $allResults += Get-ConnectionEndpoints -Environment $env

        # Get flow endpoints
        $allResults += Get-FlowEndpoints -Environment $env

        # Get app endpoints
        $allResults += Get-CanvasAppEndpoints -Environment $env

        # Get Copilot endpoints
        $allResults += Get-CopilotEndpoints -Environment $env

        Write-Host ""
    }

    Show-Summary -Results $allResults
    Export-Results -Results $allResults -Format $OutputFormat -Path $OutputPath

    Write-Host "`nDone!" -ForegroundColor Green
}

Main

#endregion
