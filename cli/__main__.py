from data_processing import create_realization as realization
from data_processing import file_paths as fp
from data_processing import forcings, gpkg_utils, graph_utils, subset

file_paths = fp.file_paths

import os
import sys
from datetime import datetime
from pathlib import Path

import cli.cli_handle as cli_handle

# Interface file

arg_options = { # name, description
    "-h": "Prints the help message",
    "[0]": "The first positional argument, the file path for the given waterbodies",
    "-s": "Creates a subset of the hydrofabric for the given waterbodies",
    "-f": "Creates forcing data for the given subset of waterbodies. Optionally can take a second argument for a config file",
    "-r": "Creates a realization for the given subset of waterbodies. Optionally can take a second argument for a config file",
    "-t": "Truncates the dataset to a smaller size, either by a ratio or by a number of waterbodies",
}

supported_filetypes = {
    ".csv": "comma separated values",
    ".txt": "plaintext, newline separated values",
    "" : "plaintext, newline separated values",
}

default_forcing_config = {
    "start_time": "2010-01-01, 12:00 AM",
    "end_time": "2010-01-02, 12:00 AM",
}

for time in ["start_time", "end_time"]:
    print(f"Converting {default_forcing_config[time]} to datetime")
    default_forcing_config[time] = datetime.strptime(default_forcing_config[time], "%Y-%m-%d, %I:%M %p")
    default_forcing_config[time] = datetime.strftime(default_forcing_config[time], "%Y-%m-%dT%H:%M")
    print(f"Converted {default_forcing_config[time]} to datetime")




def print_help():
    print("This is the help message for the command line interface")
    for key in arg_options:
        print(f"{key}: {arg_options[key]}")

def get_input_wbs():
    if len(sys.argv) < 2:
        raise Exception("No file path given")
    path = Path(sys.argv[1])
    if not path.exists():
        raise Exception(f"File {path} does not exist")
    filename = path.name
    filetype = None
    if not path.is_file():
        raise Exception(f"Path {path} is not a file")
    ext = path.suffix
    if not "." in filename:
        filetype = "plaintext"
    else:
        filetype = ext
    if filetype not in supported_filetypes:
        raise Exception(f"Filetype {filetype} not supported")
    return path, filetype

def read_input_wbs(path, filetype):
    if filetype == ".csv":
        with open(path, "r") as f:
            return f.read().split(",")
    elif filetype == ".txt" or filetype == "":
        with open(path, "r") as f:
            return f.read().split("\n")
    else:
        raise Exception(f"Filetype {filetype} not supported")
    
def get_output_foldername(ids):
    # upstream_ids = graph_utils.get_upstream_ids(ids)
    return ids[0]
    
def main():
    if "-h" in sys.argv or len(sys.argv) < 2:
        print_help()
        return
    path, filetype = get_input_wbs()
    ids = read_input_wbs(path, filetype)
    target_dir = file_paths.root_output_dir() / get_output_foldername(ids)

    if "-s" in sys.argv:
        cli_handle.subset_interface(ids)
    elif not target_dir.exists() and ("-f" in sys.argv or "-r" in sys.argv):
        raise Exception(f"No subset directory found at {target_dir}")
    
    if "-f" in sys.argv:
        f_ind = sys.argv.index("-f")
        config = None
        if len(sys.argv) > f_ind + 1 and sys.argv[f_ind + 1] not in arg_options:
            config = sys.argv[f_ind + 1]
        if config is None:
            config = default_forcing_config
        config["forcing_dir"] = target_dir.name
        cli_handle.forcings_interface(ids, config)
    
    if "-r" in sys.argv:
        r_ind = sys.argv.index("-r")
        config = None
        if len(sys.argv) > r_ind + 1 and sys.argv[r_ind + 1] not in arg_options:
            config = sys.argv[r_ind + 1]
        if config is None:
            config = default_forcing_config
        config["forcing_dir"] = target_dir.name
        cli_handle.realization_interface(ids, config)

    if "-t" in sys.argv:
        t_ind = sys.argv.index("-t")
        ratio = None
        num = None
        if len(sys.argv) > t_ind + 1 and sys.argv[t_ind + 1] not in arg_options:
            if "." in sys.argv[t_ind + 1]:
                ratio = float(sys.argv[t_ind + 1])
            else:
                num = int(sys.argv[t_ind + 1])
        cli_handle.safe_truncate(ids, ratio, num)

if __name__ == "__main__":
    main()


    

