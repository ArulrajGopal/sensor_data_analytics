# Run Model dataset
uv run src/data_generator.py --dataset Model --rows-per-file 100

# Run Variant dataset
uv run src/data_generator.py --dataset Variant --rows-per-file 500

# Run Options dataset
uv run src/data_generator.py --dataset Options --rows-per-file 1000


# uv run data_generator.py --dataset options_data_gen --num-files 5 --rows-per-file 3000
