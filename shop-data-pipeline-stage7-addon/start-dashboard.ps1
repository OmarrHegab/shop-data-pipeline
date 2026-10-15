$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
$composeFiles = @(
  '-f', '.\shop-data-pipeline-stage1\compose.yaml',
  '-f', '.\shop-data-pipeline-stage2-addon\compose.spark.yaml',
  '-f', '.\shop-data-pipeline-stage3-addon\compose.silver.yaml',
  '-f', '.\shop-data-pipeline-stage4-addon\compose.gold.yaml',
  '-f', '.\shop-data-pipeline-stage5-addon\compose.postgres.yaml',
  '-f', '.\shop-data-pipeline-stage6-addon\compose.airflow.yaml',
  '-f', '.\shop-data-pipeline-stage7-addon\compose.dashboard.yaml'
)
& docker compose @composeFiles config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Compose configuration failed. Check the seven sibling stage folders.' }
& docker compose @composeFiles up -d --build --wait --wait-timeout 300 dashboard
if ($LASTEXITCODE -ne 0) { throw 'Dashboard startup failed. Review the Docker output above.' }
Write-Host 'Dashboard ready: http://localhost:8502' -ForegroundColor Green
Write-Host 'Click Refresh warehouse data after each completed Airflow run.'
