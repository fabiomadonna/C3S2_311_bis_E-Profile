#!/bin/bash

# Loop from 2009 to 2026
for year in {2009..2026}
do
    echo "========================================================="
    echo " 🚀 START PROCESSING YEAR: $year"
    echo "========================================================="
    
    # Run the R script passing the current year as an argument.
    # Replace 'eprofile.R' with the actual name of your R file if different.
    Rscript eprofile06072026_1.R "$year"
    
    echo "========================================================="
    echo " ✅ END PROCESSING YEAR: $year (R session closed)"
    echo "========================================================="
    echo ""
done
