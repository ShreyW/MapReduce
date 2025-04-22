#!/bin/bash

BASE_DIR=$(realpath $(dirname "$0"))

# Remove the previous build directory if it exists
if [ -d "$BASE_DIR/build" ]; then
    echo "Removing existing build directory..."
    rm -rf "$BASE_DIR/build"
fi

mkdir "$BASE_DIR/build" && cd "$BASE_DIR/build"

echo "Running CMake and Make..."
cmake -S "$BASE_DIR" -B "$BASE_DIR/build" && cmake --build "$BASE_DIR/build"

# Check if cmake and make were successful
if [ $? -ne 0 ]; then
    echo "Error in building the project."
    exit 1
fi

# Ensure the bin/input directory exists
if [ ! -d "bin/input" ]; then
    echo "Creating directory bin/input..."
    mkdir -p bin/input
fi

# Copy the test/input directory
echo "Copying test input files..."
cp -r "$BASE_DIR/test/input/"* "bin/input"

echo "Setup complete. You can now run your tests."
