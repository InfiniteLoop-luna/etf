[CmdletBinding()]
param(
    [string]$TaskName = "ETF-EastmoneyAuthorDailySync",
    [string]$AuthorUid = "4348595203199492",
    [string]$Time = "18:30",
    [int]$MaxPages = 5,
    [int]$PageSize = 20,
    [int]$UnchangedPostStopCount = 10,
    [int]$OcrLimit = 50,
    [switch]$UseTesseract,
    [switch]$OcrInline,
    [switch]$EnrichPendingOcr,
    [switch]$SkipSync
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($OcrInline -and -not $UseTesseract) {
    throw "OcrInline requires UseTesseract."
}
if ($EnrichPendingOcr -and -not $UseTesseract) {
    throw "EnrichPendingOcr requires UseTesseract."
}
if ($SkipSync -and -not $EnrichPendingOcr) {
    throw "SkipSync requires EnrichPendingOcr."
}

$runnerScript = Join-Path $PSScriptRoot "run_eastmoney_author_daily.ps1"
if (-not (Test-Path -LiteralPath $runnerScript)) {
    throw "Runner script not found: $runnerScript"
}

try {
    $triggerAt = [DateTime]::Today.Add([TimeSpan]::Parse($Time))
}
catch {
    throw "Invalid Time value '$Time'. Use HH:mm, e.g. 18:30."
}

$argumentParts = @(
    "-NoProfile"
    "-ExecutionPolicy Bypass"
    ('-File "{0}"' -f $runnerScript)
    "-AuthorUid $AuthorUid"
    "-MaxPages $MaxPages"
    "-PageSize $PageSize"
    "-UnchangedPostStopCount $UnchangedPostStopCount"
    "-OcrLimit $OcrLimit"
)

if ($UseTesseract) {
    $argumentParts += "-UseTesseract"
}
if ($OcrInline) {
    $argumentParts += "-OcrInline"
}
if ($EnrichPendingOcr) {
    $argumentParts += "-EnrichPendingOcr"
}
if ($SkipSync) {
    $argumentParts += "-SkipSync"
}

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument ($argumentParts -join " ")
$trigger = New-ScheduledTaskTrigger -Daily -At $triggerAt
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Description "Daily incremental sync for Eastmoney author tracking" `
    -Force | Out-Null

Write-Host "Scheduled task created: $TaskName" -ForegroundColor Green
Write-Host "Runs daily at $Time using $runnerScript" -ForegroundColor Green
