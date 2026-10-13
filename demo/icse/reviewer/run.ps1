$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
Set-Location $PSScriptRoot
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw 'Docker Desktop is required (Linux containers mode).' }
docker info *> $null
if ($LASTEXITCODE -ne 0) { throw 'Docker Desktop is not running.' }
foreach ($line in (Get-Content READY_RUN_SHA256SUMS)) {
  if ($line -notmatch '^([0-9a-f]{64})  ([A-Za-z0-9._-]+)$') { throw 'Malformed SHA256SUMS entry' }
  $expected = $Matches[1]; $filename = $Matches[2]
  if ((Get-FileHash -Algorithm SHA256 -LiteralPath $filename).Hash.ToLowerInvariant() -ne $expected) {
    throw "Checksum mismatch: $filename"
  }
}
$image = (Get-Content IMAGE_REF -Raw).Trim()
if ($image -notmatch '^fcp-icse-reviewer:[0-9a-f]{12}$') { throw 'Unexpected image reference' }
docker load -i (Join-Path $PSScriptRoot 'image.tar.gz')
if ($LASTEXITCODE -ne 0) { throw 'Could not load the prebuilt image' }
$output = Join-Path $PSScriptRoot 'results'
New-Item -ItemType Directory -Path $output -Force | Out-Null
if (Test-Path (Join-Path $output 'PROVENANCE.json')) { throw 'Move existing results/ before another run.' }
docker run --rm --network none --cap-drop ALL --security-opt no-new-privileges --mount "type=bind,source=$PSScriptRoot,target=/input,readonly" --mount "type=bind,source=$output,target=/output" $image
if ($LASTEXITCODE -ne 0) { throw 'ICSE demonstration failed; inspect console diagnostics' }
Write-Host "PASS. Open $output\network\operator-report.html"
