[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('\A[a-f0-9]{32}\z')]
    [string]$OperationId,
    [Parameter(Mandatory = $true)]
    [string]$PythonExe,
    [Parameter(Mandatory = $true)]
    [string]$GuardScript,
    [Parameter(Mandatory = $true)]
    [string]$ConfigPath
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if (-not (Test-Path -LiteralPath $PythonExe -PathType Leaf)) {
    throw 'Guard Python interpreter is unavailable.'
}
if (-not (Test-Path -LiteralPath $GuardScript -PathType Leaf)) {
    throw 'Guard supervisor script is unavailable.'
}
if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
    throw 'Protected guard configuration is unavailable.'
}

$taskName = "FCP-Recorder-PauseResume-$OperationId"
$arguments = "-I -B `"$GuardScript`" watch --config `"$ConfigPath`""
$action = New-ScheduledTaskAction -Execute $PythonExe -Argument $arguments
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal `
    -UserId 'SYSTEM' `
    -LogonType ServiceAccount `
    -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Seconds 20) `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30) `
    -MultipleInstances IgnoreNew

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Settings $settings `
    -Description "Bounded capture resume guard for operation $OperationId" `
    -Force | Out-Null

Start-ScheduledTask -TaskName $taskName
Write-Output $taskName
