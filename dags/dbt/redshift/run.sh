#!/bin/bash

# Get the current date
date=$(date +'%Y-%m-%d')

month=$(date +'%Y%m')

# Set the output file name using the current date
output_file="logs/${month}/${date}.log"
output_dir="logs/${month}"

if [ ! -d "$output_dir" ]; then
  mkdir -p "$output_dir"
fi

# Run the command and redirect output to a file with the current date in the name
/home/datalake/.local/bin/dotenv /home/datalake/.local/bin/dbt run >> $output_file