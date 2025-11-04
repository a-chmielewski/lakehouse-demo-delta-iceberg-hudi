# PowerShell script to sync raw TLC data to S3
# Windows 11 compatible version of s3_sync_raw.sh

# Enable strict error handling
$ErrorActionPreference = "Stop"

# Load environment variables from .env file
if (Test-Path ".env") {
    Get-Content ".env" | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+?)\s*=\s*(.+?)\s*$') {
            $name = $matches[1]
            $value = $matches[2]
            # Remove quotes if present
            $value = $value.Trim('"').Trim("'")
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
            Write-Host "Loaded: $name" -ForegroundColor Green
        }
    }
} else {
    Write-Host "Error: .env file not found" -ForegroundColor Red
    exit 1
}

$S3_BUCKET = $env:S3_BUCKET
$RAW_PREFIX = $env:RAW_PREFIX
$AWS_PROFILE = $env:AWS_PROFILE

if (-not $S3_BUCKET) {
    Write-Host "Error: S3_BUCKET not set in .env file" -ForegroundColor Red
    exit 1
}

if (-not $RAW_PREFIX) {
    Write-Host "Error: RAW_PREFIX not set in .env file" -ForegroundColor Red
    exit 1
}

if (-not $AWS_PROFILE) {
    Write-Host "Error: AWS_PROFILE not set in .env file" -ForegroundColor Red
    exit 1
}

# Use Windows temp directory
$LOCAL_TLC_DIR = "$env:TEMP\tlc"

if (-not (Test-Path $LOCAL_TLC_DIR)) {
    Write-Host "Error: Local directory not found: $LOCAL_TLC_DIR" -ForegroundColor Red
    Write-Host "Please download TLC data first using: python scripts\download_tlc.py" -ForegroundColor Yellow
    exit 1
}

Write-Host "`nSyncing from: $LOCAL_TLC_DIR" -ForegroundColor Cyan
Write-Host "To S3: s3://$S3_BUCKET/$RAW_PREFIX/" -ForegroundColor Cyan

aws s3 sync $LOCAL_TLC_DIR "s3://$S3_BUCKET/$RAW_PREFIX/" --profile $AWS_PROFILE

