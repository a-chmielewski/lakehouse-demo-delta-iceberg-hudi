# PowerShell script to show S3 bucket metrics
# Windows 11 compatible version of metrics_s3.sh

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
$AWS_PROFILE = $env:AWS_PROFILE

if (-not $S3_BUCKET) {
    Write-Host "Error: S3_BUCKET not set in .env file" -ForegroundColor Red
    exit 1
}

if (-not $AWS_PROFILE) {
    Write-Host "Error: AWS_PROFILE not set in .env file" -ForegroundColor Red
    exit 1
}

Write-Host "`nListing S3 bucket: s3://$S3_BUCKET" -ForegroundColor Cyan
aws s3 ls "s3://$S3_BUCKET/" --recursive --human-readable --summarize --profile $AWS_PROFILE

