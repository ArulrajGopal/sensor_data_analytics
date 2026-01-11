# -------------------------------
# Upload CSV files to ADLS Gen2
# Uses SAS extracted from AZURE_CONNECTION_STRING
# -------------------------------

# Fail fast if connection string is missing
if (-not $env:AZURE_CONNECTION_STRING) {
    Write-Error "AZURE_CONNECTION_STRING environment variable is not set"
    exit 1
}

# Extract SAS token from connection string
$connectionString = $env:AZURE_CONNECTION_STRING

if ($connectionString -notmatch "SharedAccessSignature=") {
    Write-Error "AZURE_CONNECTION_STRING does not contain a SAS token"
    exit 1
}

$sas = "?" + (($connectionString -split "SharedAccessSignature=")[1])

# Source and destination
$sourcePath = "C:\Users\Arulraj Gopal\OneDrive\Desktop\data\Model\*.csv"
$storageAccount = "sensoranalyticsadls"
$container = "landing"
$folder = "Model"

$destinationUrl = "https://$storageAccount.dfs.core.windows.net/$container/$folder/$sas"

Write-Host "Starting upload to ADLS Gen2..."
Write-Host "Destination: https://$storageAccount.dfs.core.windows.net/$container/$folder/"

# Run AzCopy
azcopy copy `
    $sourcePath `
    $destinationUrl `
    --recursive=true `
    --overwrite=true

# Check result
if ($LASTEXITCODE -ne 0) {
    Write-Error "AzCopy upload failed"
    exit 1
}

Write-Host "Upload completed successfully ✅"
