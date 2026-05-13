[CmdletBinding()]
param(
    [string]$AuthorUid = "4348595203199492",
    [int]$MaxPages = 5,
    [int]$PageSize = 20,
    [int]$UnchangedPostStopCount = 10,
    [int]$OcrLimit = 50,
    [string]$PythonExe = "python",
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

$repoRoot = Split-Path -Parent $PSScriptRoot
$commandArgs = @(
    "-m",
    "scripts.sync_eastmoney_author",
    "--author-uid",
    $AuthorUid,
    "--max-pages",
    "$MaxPages",
    "--page-size",
    "$PageSize",
    "--unchanged-post-stop-count",
    "$UnchangedPostStopCount",
    "--ocr-limit",
    "$OcrLimit"
)

if ($UseTesseract) {
    $commandArgs += "--use-tesseract"
}
if ($OcrInline) {
    $commandArgs += "--ocr-inline"
}
if ($EnrichPendingOcr) {
    $commandArgs += "--enrich-pending-ocr"
}
if ($SkipSync) {
    $commandArgs += "--skip-sync"
}

Push-Location $repoRoot
try {
    Write-Host "Running Eastmoney author sync from $repoRoot" -ForegroundColor Cyan
    Write-Host "$PythonExe $($commandArgs -join ' ')" -ForegroundColor DarkGray
    & $PythonExe @commandArgs
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
