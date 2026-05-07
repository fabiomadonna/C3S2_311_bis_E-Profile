#!/bin/bash

# Navigate to the working directory
cd /home/fabiomadonna/eprofile/bck/e-profile/

# Loop from 2009 to 2025
for year in {2009..2025}
do
    echo "============================================"
    echo "Starting processing for year: $year"
    echo "============================================"

    # Run the R script and pass the year as a string
    Rscript eprofile27042026.R "$year"

    # Brief pause to allow the process to finish
    sleep 2

    # Forcefully kill any remaining R processes to free up memory
    echo "Cleaning up memory: killing all R processes..."
    sudo pkill -9 R

    # Short break before starting the next year
    sleep 1
done

echo "Batch processing completed successfully."

