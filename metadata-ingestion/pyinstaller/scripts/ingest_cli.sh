#!/bin/bash

# Move to the project root directory
projectRoot="/home/zeta/git/datahub/metadata-ingestion"
cd "$projectRoot"

# Function to remove items with error handling
remove_safely() {
    if [ -e "$1" ]; then
        rm -rf "$1" || echo "Warning: Could not remove $1. Continuing anyway."
    fi
}

# Clean up previous build artifacts while preserving important files
remove_safely "build/ingest_cli"
remove_safely "dist/ingest_cli"

# Activate virtual environment and execute commands
source venv/bin/activate

if [ $? -eq 0 ]; then
    # Generate binary using PyInstaller
    pyinstaller --clean pyinstaller/spec/ingest_cli.spec

    if [ $? -eq 0 ]; then
        outputPath="$projectRoot/dist/ingest_cli"
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