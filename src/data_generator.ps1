# -------------------------------
# Data Generate
# -------------------------------
uv run src/data_generator.py --dataset Model --rows-per-file 100

uv run src/data_generator.py --dataset Variant --rows-per-file 500

uv run src/data_generator.py --dataset Options --rows-per-file 1000

uv run src/data_generator.py --dataset VehicleInstance --rows-per-file 40000000
# -------------------------------
# Upload CSV files to ADLS Gen2
# Uses SAS extracted from AZURE_CONNECTION_STRING
# -------------------------------

if (-not $env:AZURE_CONNECTION_STRING) {
    Write-Error "AZURE_CONNECTION_STRING environment variable is not set"
    exit 1
}


$connectionString = $env:AZURE_CONNECTION_STRING

if ($connectionString -notmatch "SharedAccessSignature=") {
    Write-Error "AZURE_CONNECTION_STRING does not contain a SAS token"
    exit 1
}

$sas = "?" + (($connectionString -split "SharedAccessSignature=")[1])

$sourcePath = "C:\Users\Arulraj Gopal\OneDrive\Desktop\data\*.csv"
$storageAccount = "sensoranalyticsadls"
$container = "landing"
$folder = "data"

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

Write-Host "Upload completed successfully "

# uv run data_generator.py --dataset options_data_gen --num-files 5 --rows-per-file 3000
