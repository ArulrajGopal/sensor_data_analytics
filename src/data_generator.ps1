# Run Model dataset
uv run src/data_generator.py --dataset Model --rows-per-file 100

# Run Variant dataset
uv run src/data_generator.py --dataset Variant --rows-per-file 500

# Run Options dataset
uv run src/data_generator.py --dataset Options --rows-per-file 1000

# $env:AZCOPY_ACCOUNT_KEY = $env:STORAGE_ACCOUNT_KEY

# azcopy copy `
#   "C:\Users\Arulraj Gopal\OneDrive\Desktop\data\Model\*.csv" `
#   "https://sensoranalytics.dfs.core.windows.net/landing/Model/" `
#   --recursive=true


# uv run data_generator.py --dataset options_data_gen --num-files 5 --rows-per-file 3000
