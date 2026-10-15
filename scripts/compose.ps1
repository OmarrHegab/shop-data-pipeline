# Dot-source from PowerShell: . .\scripts\compose.ps1
# Defines the complete Compose file list in the current shell.
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot
$composeFiles = @(
    '-f', '.\shop-data-pipeline-stage1\compose.yaml',
    '-f', '.\shop-data-pipeline-stage2-addon\compose.spark.yaml',
    '-f', '.\shop-data-pipeline-stage3-addon\compose.silver.yaml',
    '-f', '.\shop-data-pipeline-stage4-addon\compose.gold.yaml',
    '-f', '.\shop-data-pipeline-stage5-addon\compose.postgres.yaml',
    '-f', '.\shop-data-pipeline-stage6-addon\compose.airflow.yaml',
    '-f', '.\shop-data-pipeline-stage7-addon\compose.dashboard.yaml'
)
