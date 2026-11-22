<#
.SYNOPSIS
    Points the Power BI parameter GoldFolder at this repository and opens the Parfois Dashboard.

.DESCRIPTION
    Power Query only accepts absolute file paths, so a .pbip cannot read "..\data\exports\powerbi"
    by itself. This script finds the folder it lives in (the repository root), writes
    <root>\data\exports\powerbi\ into the GoldFolder parameter of the semantic model
    (Parfois Dashboard.SemanticModel\definition\expressions.tmdl) and opens the project.
    Run it from any copy of the repository, on any machine, instead of opening the .pbip directly.
    After it opens, click Home > Refresh when the model has no data yet (cache.abf is not in Git).

.PARAMETER NoOpen
    Only update the parameter; do not start Power BI Desktop.
#>
param([switch]$NoOpen)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$dataFolder = Join-Path $root 'data\exports\powerbi\'
$pbip = Join-Path $root 'Parfois Dashboard.pbip'
$expressions = Join-Path $root 'Parfois Dashboard.SemanticModel\definition\expressions.tmdl'

$open = Get-Process PBIDesktop -ErrorAction SilentlyContinue | Where-Object { $_.MainWindowTitle -like 'Parfois Dashboard*' }
if ($open) {
    Write-Warning 'Parfois Dashboard is open in Power BI Desktop. Close it first: Desktop keeps its own copy of the parameter and would overwrite this change when saving.'
    exit 1
}

$text = [IO.File]::ReadAllText($expressions)
$pattern = '(expression\s+GoldFolder\s*=\s*")[^"]*(")'
if ($text -notmatch $pattern) { throw "GoldFolder parameter not found in $expressions" }
$updated = [regex]::Replace($text, $pattern, { param($m) $m.Groups[1].Value + $dataFolder + $m.Groups[2].Value })
if ($updated -ne $text) {
    [IO.File]::WriteAllText($expressions, $updated, (New-Object Text.UTF8Encoding $false))
    Write-Host "GoldFolder -> $dataFolder"
} else {
    Write-Host "GoldFolder already points to $dataFolder"
}

if (-not (Test-Path (Join-Path $dataFolder 'gld_fact_sales.parquet'))) {
    Write-Warning "No Parquet files in $dataFolder yet. Run the gold_export_powerbi DAG (or copy the export there) before refreshing."
}

if (-not $NoOpen) { Start-Process -FilePath $pbip }
