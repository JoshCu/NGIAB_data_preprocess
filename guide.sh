#!/bin/bash

# Color definitions
BBlack='\033[1;30m'
BRed='\033[1;31m'
BGreen='\033[1;32m'
BYellow='\033[1;33m'
BBlue='\033[1;34m'
BPurple='\033[1;35m'
BCyan='\033[1;36m'
BWhite='\033[1;37m'
UBlack='\033[4;30m'
URed='\033[4;31m'
UGreen='\033[4;32m'
UYellow='\033[4;33m'
UBlue='\033[4;34m'
UPurple='\033[4;35m'
UCyan='\033[4;36m'
UWhite='\033[4;37m'
Color_Off='\033[0m'

set -e

CONFIG_FILE="$HOME/.host_data_path.conf"

echo -e "\n========================================================="
echo -e "${UWhite} Welcome to CIROH-UA:NextGen National Water Model App! ${Color_Off}"
echo -e "=========================================================\n"
echo -e "Looking for input data (a directory containing the following directories: forcings, config and outputs):\n"
echo -e "${BBlue}forcings${Color_Off} is the hydrofabric input data for your model(s)."
echo -e "${BGreen}config${Color_Off} folder has all the configuration related files for the model."
echo -e "${BPurple}outputs${Color_Off} is where the output files are copied to when the model finish the run"

echo -e "\n"

# Check if the config file exists and read from it
if [ -f "$CONFIG_FILE" ]; then
    LAST_PATH=$(cat "$CONFIG_FILE")
    echo -e "Last used data directory path: ${BBlue}$LAST_PATH${Color_Off}"
    read -erp "Do you want to use the same path? (Y/n): " use_last_path
    if [[ "$use_last_path" != [Nn]* ]]; then
        HOST_DATA_PATH=$LAST_PATH
    else
        read -erp "Enter your input data directory path (use absolute path): " HOST_DATA_PATH
    fi
else
    read -erp "Enter your input data directory path (use absolute path): " HOST_DATA_PATH
fi

# Check the directory exists
if [ ! -d "$HOST_DATA_PATH" ]; then
  echo -e "${BRed}Directory does not exist. Exiting the program.${Color_Off}"
  exit 0
fi

# Save the new path to the config file
echo "$HOST_DATA_PATH" > "$CONFIG_FILE"
echo -e "The Directory you've given is:\n$HOST_DATA_PATH\n"
# Function to validate directories
validate_directory() {
    local dir=$1
    local name=$2
    local color=$3

    if [ -d "$dir" ]; then
        local count=$(ls -R "$dir" | wc -l)
        echo -e "${color}${name}${Color_Off} exists. $count ${name} found."
    else
        echo -e "Error: Directory $dir does not exist."
    fi
}

validate_directory "$HOST_DATA_PATH/forcings" "forcings" "$BBlue"
validate_directory "$HOST_DATA_PATH/config" "config" "$BGreen"
validate_directory "$HOST_DATA_PATH/outputs" "outputs" "$BPurple"

# Function to perform cleanup
cleanup_folder() {
    local folder_path="$1"
    local file_types="$2"
    local folder_name="$3"

    # Construct the find command
    local find_cmd="find \"$folder_path\" -maxdepth 2 -type f \( $file_types \)"

    # Execute the find command and count the results
    local file_count=$(eval "$find_cmd" 2> /dev/null | wc -l)

    echo "Files found: $file_count"

    if [ "$file_count" -gt 0 ]; then
        echo -e "${UYellow}Cleanup Process: matching files ($file_types) in $folder_name: $folder_path${Color_Off}"
        echo -e "Select an option (type a number): "
        choose_option
    else
        echo "$folder_name is ready for run. No matching files found."
    fi
}

choose_option() {
    options=("Delete files and run fresh" "Continue without cleaning" "Exit")
    select option in "${options[@]}"; do
        case $option in
            "Delete files and run fresh")
                echo "Cleaning folder for fresh run"

                # Construct the find delete command
                local find_delete_cmd="find \"$folder_path\" -maxdepth 2 -type f \( $file_types \) -delete"

                # Execute the find delete command
                eval "$find_delete_cmd"
                break
                ;;
            "Continue without cleaning")
                echo "Continuing with existing files."
                break
                ;;
            "Exit")
                echo "Exiting script. Have a nice day!"
                exit 0
                ;;
            *) echo "Invalid option $REPLY. Please select again."
                ;;
        esac
    done
}


# Cleanup Process for Outputs Folder
cleanup_folder "$HOST_DATA_PATH/outputs/" "-name '*' " "Outputs"

# File discovery
echo -e "\nLooking in the provided directory gives us:"
find_files() {
    local path=$1
    local name=$2
    local color=$3

    local files=$(find "$path" -iname "*$name*.*")
    echo -e "${color}Found these $name files:${Color_Off}"
    echo "$files" || echo "No $name files found."
}

find_files "$HOST_DATA_PATH" "catchment" "$UGreen"
find_files "$HOST_DATA_PATH" "nexus" "$UGreen"
find_files "$HOST_DATA_PATH" "realization" "$UGreen"

# Detect Arch and Docker
echo -e "\nDetected ISA = $(uname -a)"
if docker --version ; then
    echo "Docker found"
else 
    echo "Docker not found"
fi 

IMAGE_NAME="joshcu/ngiab_workshop_demo"


# Model run options
echo -e "\n${URed}WARNING: THIS IS A DEMO VERSION AND WILL NOT BE UPDATED ${Color_Off}\n"
echo -e "This is a demo version of the NextGen In A Box: National Water Model."
sleep 3


echo -e "\nRunning NextGen docker container..."
echo -e "Mounting local host directory $HOST_DATA_PATH to /ngen/ngen/data within the container."
docker run --rm -it -v "$HOST_DATA_PATH:/ngen/ngen/data" "$IMAGE_NAME" /ngen/ngen/data/

# Final output count
Final_Outputs_Count=$(ls "$HOST_DATA_PATH/outputs" | wc -l)
echo -e "$Final_Outputs_Count new outputs created."
echo -e "Any copied files can be found here: $HOST_DATA_PATH/outputs"
echo -e "Thank you for running NextGen In A Box: National Water Model! Have a nice day!"
exit 0