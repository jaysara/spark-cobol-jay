#!/bin/bash

# Paths to the input and output directories
original_path="path/to/original/parquet"
optimized_path="path/to/optimized/parquet"

# Get the size of the original directory
original_size=$(du -sb $original_path | cut -f1)

# Get the size of the optimized directory
optimized_size=$(du -sb $optimized_path | cut -f1)

# Calculate the size difference
size_difference=$((original_size - optimized_size))

# Print the results
echo "Original Size: $original_size bytes"
echo "Optimized Size: $optimized_size bytes"
echo "Size Difference: $size_difference bytes"
