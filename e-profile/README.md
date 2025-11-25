# ECMWF Software with Documentation (SD) Deliverable

**Project / Activity Name:** E-PROFILE Wind Profilers' Data Processing System
**Version:** 1.0.0
**Date:** 2025-11-24
**Author(s):** Faezeh Karimian Saracks, Fabio Madonna, Emanuele Tramutola

---

## 1. Purpose
This document includes workflow explanations, expanded scientific background, block-diagram visualizations, and technical commentary on each component
of the R scripts used to retrieve and process the E-PROFILE wind profilers' data and obtained the time-aggregated version of the datasets, specifically designed for the 
Copernicus Data Store.

The processing workflow performs: metadata extraction 
- data access and download (including handling of certificates to access the data)
- raw data extraction and filtering 
- cleaning 
- computation of wind metrics 
- multiple aggregation levels (hourly/daily/monthly/yearly)
- NetCDF export per station and height level.

EUMETNET's E-PROFILE programme (https://e-profile.eu/) is specifically dedicated to the collection, processing, and dissemination of wind profiler data. 
This program aims to create a continuous and reliable observation network of wind profiles, which is essential for weather forecasting, climate modeling, 
and emergency management. The main goal is to improve the overall usability of wind profiler data for operational meteorology and 
to provide support and expertise to both profiler operators and end users. 

---

## 2. Audience
This guide is intended for: 
• Atmospheric scientists 
• Data engineers working with large observational datasets 
• Research groups performing climate
reanalysis 
• Renewable energy specialists analysing wind behaviour.

---

## 3. Software Repository
**Repository URL:** [https://github.com/fabiomadonna/C3S2_311_bis/e-profile]

**Repository Structure:**

- `config/` - Configuration files
- `converters/` - Data transformation logic
- `database/` - Database operations and migration scripts
- `processors/` - Core data processing logic
- `readers/` - NetCDF file readers
- `tests/` - Unit and integration tests
- `utils/` - Utility functions (logging, station management)
- `main.py` - Main entry point
- `requirements.txt` - Python dependencies
- `README.md` - Quick start guide
- `SoftwareUserGuide.md` - Detailed user manual

**License:** CC-BY-4.0



2. OVERVIEW OF THE PROCESSING PIPELINE

3. WORKFLOW SCHEME (TEXT DIAGRAM)
RAW CSV FILES
¯ Metadata extraction (parallel)
¯ Data extraction: read ® parse ® filter blocks ® unify
¯ Filtering step (implausible values, QC)
¯ Compute wind variables:
- wind speed
- wind direction
- wind power density
- turbulence intensity
- wind stress
¯ Aggregation:
® hourly
® daily (24-hour completeness check)
® monthly
® yearly
¯ Normalize time fields
¯ NetCDF export (station × height × period)
¯ Logs saved
4. DETAILED EXPLANATION OF MAIN SCRIPT SECTIONS
4.1 Package loading
The script automatically installs and loads all required packages, ensuring portability.
4.2 Metadata extraction
Each CSV may contain one or more header blocks. The system reads them in parallel and
produces a
metadata summary table grouped by station.
4.3 File processing
Each file is scanned to identify “data” blocks. Within each block:
- station and date_valid fields are extracted
- the block is parsed via data.table::fread
- variables X1, X4, X6, X7 are retained
- X4 is filtered for quality control
- logs count retained vs removed observations
4.4 Parallel extraction
Files are split into batches and computed with foreach/doParallel.
A global filtered dataset is produced and sorted.
4.5 Cleaning and physical variable computation
Wind speed = sqrt(u² + v²)
Wind direction = atan2(u, v)
Wind power density = ½ · r · U³
Turbine flags (cut-in, cut-out limits) are added.
4.6 Filtering steps
The script logs the impact of each filtering step at each height and station.
4.7 Aggregation workflow
– Hourly: statistical summaries including SD, turbulence intensity, wind run
– Daily: requires 24 hourly values
– Monthly: averages and extrema on daily values
– Yearly: averages and extrema on monthly values
4.8 NetCDF export
For each period (hourly/daily/monthly/yearly) and station:
- Long-format conversion
- Creation of NetCDF variables (values, timestamps, heights)
- Addition of metadata and CF-compliant attributes
5. ADDITIONAL RECOMMENDATIONS
• Keep metadata Excel file updated to avoid missing attributes in NetCDF.
• Prefer HPC clusters for >10 GB datasets.
• Ensure date formats are consistent.
6. FINAL NOTES
This extended document is designed to serve as the complete reference for both operational and
scientific
usage of the E-PROFILE wind profiler workflow.
