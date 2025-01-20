#!/bin/bash

# Move to the project root directory
projectRoot=$HOME/centos7-docker/datahub/metadata-ingestion
cd "$projectRoot"

# Function to remove items with error handling
remove_safely() {
    if [ -e "$1" ]; then
        rm -rf "$1" || echo "Warning: Could not remove $1. Continuing anyway."
    fi
}

# Clean up previous build artifacts while preserving important files
remove_safely "build/async_lite_gms"
remove_safely "dist/async_lite_gms"

# Activate virtual environment and execute commands
source venv/bin/activate

if [ $? -eq 0 ]; then
    # Generate binary using PyInstaller
    LD_LIBRARY_PATH=/usr/local/lib pyinstaller --clean pyinstaller/spec/async_lite_gms.spec

    if [ $? -eq 0 ]; then
        outputPath="$projectRoot/dist/async_lite_gms"
        echo -e "\e[32mBuild completed successfully.\e[0m"
        echo -e "\e[32mThe final executable is located at:\e[0m"
        echo -e "\e[36m$outputPath\e[0m"
    else
        echo -e "\e[31mAn error occurred during the PyInstaller build process.\e[0m"
    fi

    # Deactivate virtual environment
    deactivate
else
    echo -e "\e[31mFailed to activate the virtual environment.\e[0m"
fi

# Check for overall script success
if [ $? -ne 0 ]; then
    echo -e "\e[31mAn error occurred during the build process.\e[0m"
    exit 1
fi
