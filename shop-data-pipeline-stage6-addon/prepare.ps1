$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot
$composeFiles = @(
    "-f", ".\shop-data-pipeline-stage1\compose.yaml",
    "-f", ".\shop-data-pipeline-stage2-addon\compose.spark.yaml",
    "-f", ".\shop-data-pipeline-stage3-addon\compose.silver.yaml",
    "-f", ".\shop-data-pipeline-stage4-addon\compose.gold.yaml",
    "-f", ".\shop-data-pipeline-stage5-addon\compose.postgres.yaml",
    "-f", ".\shop-data-pipeline-stage6-addon\compose.airflow.yaml"
)
function Invoke-Compose {
    param([string[]]$Arguments)
    & docker compose @composeFiles @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed: $($Arguments -join ' ')" }
}
foreach ($stage in 1..6) {
    $folder = if ($stage -eq 1) { "shop-data-pipeline-stage1" } else { "shop-data-pipeline-stage$stage-addon" }
    if (!(Test-Path $folder)) { throw "Missing sibling folder: $folder" }
}
$configDir = Join-Path $PSScriptRoot "config"
New-Item -ItemType Directory -Force -Path $configDir | Out-Null
Invoke-Compose -Arguments @("config", "--quiet")
Invoke-Compose -Arguments @("build", "warehouse", "airflow")
Invoke-Compose -Arguments @("up", "-d", "--wait", "kafka", "postgres", "airflow-db")
Invoke-Compose -Arguments @("run", "--rm", "--no-deps", "spark-storage-init")
# Stopped service containers provide authoritative daemon-side mount paths.
Invoke-Compose -Arguments @("create", "spark", "silver", "gold", "warehouse")
$ids = @(& docker compose @composeFiles ps --all --quiet spark silver gold warehouse)
if ($LASTEXITCODE -ne 0 -or $ids.Count -ne 4) { throw "Expected four job template containers." }
$inspectLines = & docker inspect @ids
if ($LASTEXITCODE -ne 0) { throw "Could not inspect job templates." }
$inputPath = Join-Path $configDir "templates.json"
[System.IO.File]::WriteAllText($inputPath, ($inspectLines -join "`n"), [System.Text.UTF8Encoding]::new($false))
# The read-only mount is for normal Airflow runs; export to stdout then save on Windows.
$jsonLines = & docker compose @composeFiles run --rm --no-deps --entrypoint python airflow /opt/project/airflow/export_config.py /opt/project/job-config/templates.json -
if ($LASTEXITCODE -ne 0) { throw "Could not export job settings." }
$parsedJobs = ($jsonLines -join "`n") | ConvertFrom-Json
if ($parsedJobs.version -ne 1) { throw "Unexpected job settings format." }
[System.IO.File]::WriteAllText((Join-Path $configDir "jobs.json"), ($jsonLines -join "`n"), [System.Text.UTF8Encoding]::new($false))
Remove-Item $inputPath
Invoke-Compose -Arguments @("run", "--rm", "--no-deps", "airflow", "python", "/opt/project/airflow/check_setup.py")
Write-Host "AIRFLOW PREPARATION COMPLETE"
