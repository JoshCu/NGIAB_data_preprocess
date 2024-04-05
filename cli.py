import argparse
from data_processing.file_paths import file_paths
from data_processing.subset import subset
from data_processing.forcings import create_forcings
from data_processing.create_realization import create_realization
import os
import sys
from datetime import datetime
from pathlib import Path


def main():
    # Create the parser
    my_parser = argparse.ArgumentParser(
        description="Subsetting hydrofabrics, forcing generation, and realization creation"
    )

    # Add input arguments
    my_parser.add_argument(
        "-i",
        "--input_file",
        type=str,
        help="Path to a csv or txt file containing a list of waterbody IDs",
    )
    my_parser.add_argument(
        "-s",
        "--subset",
        action="store_true",
        help="Subset the hydrofabric to the given waterbody IDs",
    )
    my_parser.add_argument(
        "-f",
        "--forcings",
        action="store_true",
        help="Generate forcings for the given waterbody IDs",
    )
    my_parser.add_argument(
        "-r",
        "--realization",
        action="store_true",
        help="Create a realization for the given waterbody IDs",
    )
    my_parser.add_argument(
        "--start_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        help="Start date for forcings/realization (format YYYY-MM-DD)",
    )
    my_parser.add_argument(
        "--end_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        help="End date for forcings/realization (format YYYY-MM-DD)",
    )
    my_parser.add_argument(
        "-o",
        "--output_name",
        type=str,
        help="Name of the subset to be created (default is the first waterbody ID in the input file)",
    )

    # Execute the parse_args() method
    args = my_parser.parse_args()
    if not any([args.subset, args.forcings, args.realization]):
        print("At least one of --subset, --forcings, or --realization must be set.")
        sys.exit()
    if args.subset:
        input_file = Path(args.input_file)

        # Check if the input file exists
        if not input_file.exists():
            print(f"The file {input_file} does not exist")
            sys.exit()

        # Validate the file type
        if input_file.suffix not in [".csv", ".txt", ""]:
            print(f"Unsupported file type: {input_file.suffix}")
            sys.exit()

        # Read in the waterbody IDs
        with input_file.open("r") as f:
            waterbody_ids = f.read().splitlines()
    if args.output_name:
        wb_id_for_name = args.output_name
    elif waterbody_ids:
        wb_id_for_name = waterbody_ids[0]
    else:
        print("No waterbody input file or output folder provided.")
        sys.exit()
    paths = file_paths(wb_id_for_name)
    output_folder = paths.subset_dir()

    # Create the output folder if it does not exist
    if not output_folder.exists():
        output_folder.mkdir(parents=True)

    # If the subset flag is set, run the subset function
    if args.subset:
        subset(waterbody_ids, subset_name=wb_id_for_name)

    # If the forcings flag is set, run the forcings function
    if args.forcings:
        if not args.start_date or not args.end_date:
            print("Forcings generation requires both --start_date and --end_date to be provided.")
            sys.exit()
        create_forcings(
            start_time=args.start_date, end_time=args.end_date, output_folder_name=wb_id_for_name
        )

    # If the realization flag is set, run the realization function
    if args.realization:
        if not args.start_date or not args.end_date:
            print("Realization creation requires both --start_date and --end_date to be provided.")
            sys.exit()
        create_realization(wb_id_for_name, start_time=args.start_date, end_time=args.end_date)


if __name__ == "__main__":
    main()
