import time
import os
import argparse
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from data_gen_utils import *

DATASET_MAP = {
    "Model": model_data_gen,
    "Options": options_data_gen,
    "Variant":variant_data_gen,
    "VehicleInstance":vehile_instance_data_gen
}




def main(dataset_name, num_files, max_workers, rows_per_file):

    # Resolve dataset function
    dataset_func = DATASET_MAP.get(dataset_name)

    if dataset_func is None:
        raise ValueError(
            f"Unknown dataset '{dataset_name}'. "
            f"Available datasets: {list(DATASET_MAP.keys())}"
        )

    start_time = time.time()

    print(
        f"Generating {num_files} files with "
        f"{rows_per_file:,} rows each "
        f"using {max_workers} workers..."
    )

    # Bind rows_per_file → function
    gen_func = partial(dataset_func, rows=rows_per_file)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        output_paths = list(
            executor.map(gen_func, range(1, num_files + 1))
        )

    elapsed_time = time.time() - start_time

    total_size_bytes = sum(os.path.getsize(path) for path in output_paths)
    total_size_gb = total_size_bytes / (1024 ** 3)


    print(f"\n{'=' * 60}")
    print(f"Dataset       : {dataset_name}")
    print(f"Total time    : {elapsed_time:.2f} seconds")
    print(f"Total size    : {total_size_gb:.4f} GB")

    print(f"{'=' * 60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate random CSV files in parallel")
    parser.add_argument("--dataset", type=str,required=True,help=f"Dataset generator to use. Options: {list(DATASET_MAP.keys())}")
    parser.add_argument("--num-files",type=int,default=1)
    parser.add_argument("--max-workers",type=int,default=4)
    parser.add_argument("--rows-per-file",type=int,default=5000)

    args = parser.parse_args()

    main(
        dataset_name=args.dataset,
        num_files=args.num_files,
        max_workers=args.max_workers,
        rows_per_file=args.rows_per_file
    )



