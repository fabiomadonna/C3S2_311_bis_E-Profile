# ECMWF Software with Documentation (SD) Deliverable

**Project / Activity Name:** E-PROFILE Wind Profilers' Data Processing System
- Version:1.0.0
-Date: 2025-11-24
-Author(s): Faezeh Karimian Saracks, Fabio Madonna, Emanuele Tramutola

---

## 1. Purpose
This document includes workflow explanations, expanded scientific background, block-diagram visualizations, and technical commentary on each component
of the R scripts used to retrieve and process the E-PROFILE wind profilers' data and obtained the time-aggregated version of the datasets, specifically designed for the 
Copernicus Data Store.

The processing workflow performs: metadata extraction 
- data access and download (including handling of certificates to access the data)
- raw data extraction and filtering 
- multiple time aggregation levels (hourly/daily/monthly/yearly)
- computation of wind metrics 
- NetCDF export per station and height level, consistently with the C3S CDM-OBS core.

EUMETNET's E-PROFILE programme (https://e-profile.eu/) is specifically dedicated to the collection, processing, and dissemination of wind profiler data. 
This program aims to create a continuous and reliable observation network of wind profiles, which is essential for weather forecasting, climate modeling, 
and emergency management. The main goal is to improve the overall usability of wind profiler data for operational meteorology and 
to provide support and expertise to both profiler operators and end users. 

<img width="969" height="990" alt="e-profile" src="https://github.com/user-attachments/assets/af3b98de-e3b4-42f5-8c1d-06fc12702f8c" />

Figure 1: Distribution of E-PROFILE sites measuring wind, as of July 2024. In red the wind profiles network, provided in the CDS.

---

## 2. Audience
This guide is intended for:
- Atmospheric scientists
- Data engineers working with large observational datasets
- Research groups performing climate reanalysis
- Renewable energy specialists analysing wind behaviour.

---

## 3. Software Repository
**Repository URL:** [https://github.com/fabiomadonna/C3S2_311_bis/e-profile]

**Repository Structure:**
```
E-PROFILE/
├── README.md
├── eprofile.sh              # Bash script to download CEDA WINPRO datasets
├── eprofile.R               # R script for metadata extraction, filtering, aggregation, NetCDF creation
├── Data_eprofile/           # Raw and downloaded WINPRO CSV files
├── metadata/
│   └── FinalCheck_datevalid_distinct_merged_metadata.xlsx   # Station metadata
├── logs/                    # Processing and filtering logs
└── output/                  # Aggregated CSVs and NetCDF files
```

This repository contains the scripts and input files needed to download, process, and aggregate E-PROFILE wind profiler data. The structure is as follows.


### Notes

- **Data_eprofile/**: contains all CSV input files to be processed by `e-profile.R`. These are typically downloaded using `eprofile.sh`.  
- **metadata/FinalCheck_datevalid_distinct_merged_metadata.xlsx**: provides station metadata such as location, height, and instrument details, which are also provided in the global attributes of the NetCDF files. 
- **logs/**: stores filtering and QC logs generated during processing.  
- **output/**: stores processed CSV and NetCDF outputs.  
- **external/**: any external repository clones (e.g., `online_ca_client`) or certificate files.  
- **scripts/**: additional R or Bash helper scripts if needed.  


**License:** CC-BY-4.0

---

## 4. Technical Specifications

**Programming Language:**  
- R 4.3+ (tested with R 4.3.1)  
- Bash 4+ (for `eprofile.sh`)

**Package Management / Environment:**  
- R packages: `readr`, `dplyr`, `data.table`, `lubridate`, `foreach`, `doParallel`, `fasttime`, `readxl`, `magrittr`, `ncdf4`, `writexl`, `zoo`, `parallel`  
- Bash dependencies: `git`, `wget`, `curl`  

**Supported Operating Systems:**  
- Linux (tested on Ubuntu 20.04)  
- macOS  
- Windows (via R for Windows and Git Bash / WSL for Bash scripts)  

**System Requirements:**  
- **CPU:** Multi-core processor recommended for parallel processing  
- **RAM:** 8 GB minimum recommended (depends on data size)  
- **Storage:** Depends on the volume of WINPRO data being downloaded (tens of GB possible)  
- **Network:** Internet connection required for `eprofile.sh` to download datasets from CEDA  

**Notes:**  
- All processing is done locally; no external database is required.  
- NetCDF outputs comply with CF-1.8 conventions.  
---

## 5. Installation Instructions

### Prerequisites

Before running the scripts, ensure the following are installed on your system:

**1. R Environment:**  
- R 4.3+  
- R packages: `readr`, `dplyr`, `data.table`, `lubridate`, `foreach`, `doParallel`, `fasttime`, `readxl`, `magrittr`, `ncdf4`, `writexl`, `zoo`, `parallel`  
- Packages can be installed from CRAN using the `install.packages()` function.

**2. Bash Environment:**  
- Bash 4+ (comes pre-installed on most Linux/macOS systems; Windows users can use Git Bash or WSL)  
- Utilities: `git`, `wget`, `curl`

**3. Internet Connection:**  
- Required to download wind profilers' data using the script `eprofile.sh` from the CEDA repository.

**4. File Permissions:**  
- Ensure read/write permissions for directories:  
  - `Data_eprofile/` (input CSVs)  
  - `metadata/` (station metadata)  
  - `logs/` (processing logs)  
  - `output/` (aggregated CSV and NetCDF files)  

**Optional:**  
- Enough disk space for storing downloaded data and NetCDF outputs (tens of GB depending on the dataset size).  

___

**6. Installation Steps:**

Follow these steps to set up the environment and prepare the repository for processing:

**Clone the repository**

git clone https://github.com/yourusername/E-PROFILE.git

cd E-PROFILE

**Prepare directories**
Ensure the following directories exist (the scripts will create them if missing):
mkdir -p Data_eprofile logs output metadata external

**Install R packages**
Open R or RStudio and run:
required_packages <- c(
  "readr","dplyr","parallel","lubridate","foreach","doParallel",
  "fasttime","data.table","readxl","magrittr","ncdf4","writexl","zoo","parallel"
)
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, dependencies = TRUE)
  }
}

**Download WINPRO data with Bash script**
bash eprofile.sh
This will populate Data_eprofile/ with raw CSV files.
Certificates or external tools may be stored in external/.

## Obtaining a CEDA Token
To access E-Profile data from the CEDA Archive, an OAuth token is required. The token is used for authentication when downloading data.
### Steps to Obtain a Token
**Register and Log In**
   - Go to the [CEDA Archive](https://archive.ceda.ac.uk/).
   - Register for an account if you do not have one.
   - Log in to your account.
**Request OAuth Credentials**
   - Navigate to the OAuth clients page: [CEDA OAuth Clients](https://auth.ceda.ac.uk/oauth2/clients).
   - Request a new client ID and client secret.
   - Note down the `CLIENT_ID` and `CLIENT_SECRET`. The token endpoint is usually:
     ```
     https://auth.ceda.ac.uk/oauth2/token
     ```
**Generate a Token**
   You can generate a token using `curl`:
   ```bash
   curl -X POST https://auth.ceda.ac.uk/oauth2/token \
        -d "grant_type=client_credentials" \
        -u "CLIENT_ID:CLIENT_SECRET"

The response contains your access_token
{
  "access_token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "Bearer",
  "expires_in": 3600
}
Alternatively, use Python:
import requests

client_id = "YOUR_CLIENT_ID"
client_secret = "YOUR_CLIENT_SECRET"
token_url = "https://auth.ceda.ac.uk/oauth2/token"

response = requests.post(token_url,
                         data={"grant_type": "client_credentials"},
                         auth=(client_id, client_secret))

token = response.json()["access_token"]
print(token)

---

**Process data with R script**
source("eprofile.R")
Outputs (aggregated CSVs and NetCDF) will be saved in output/.

Logs will be saved in logs/.

Verify outputs:
Check output/ for generated CSV and NetCDF files. Logs in logs/ can be used to verify filtering and QC steps.

---

**7. WORKFLOW SCHEME (TEXT VERSION) AND DETAILED EXPLANATION OF MAIN SCRIPT SECTIONS**
<img width="1008" height="482" alt="Screenshot 2025-11-27 alle 12 56 37" src="https://github.com/user-attachments/assets/836bb73a-c869-48f3-9fda-dde555143ec8" />

RAW CSV FILES
¯ Metadata extraction (parallel)
¯ Data extraction: read, parse, filter blocks, unify
¯ Filtering step (implausible values, QC)
¯ Compute wind variables:
- wind speed
- wind direction
- wind power density
- turbulence intensity
- wind stress
- Aggregation:
  ® hourly
  ® daily (24-hour completeness check)
  ® monthly
  ® yearly
¯ Normalize time fields
¯ NetCDF export (dimensions = station × height × time)
¯ Logs saved

7.1 Package loading
The script automatically installs and loads all required packages, ensuring portability.

7.2 Metadata extraction
Each CSV may contain one or more header blocks. The system reads them in parallel and
produces a metadata summary table grouped by station.

7.3 File processing
Each file is scanned to identify “data” blocks. Within each block:
- station and date_valid fields are extracted
- the block is parsed via data.table::fread
- variables X1, X4, X6, X7 are retained, these are Quality flags applied to the wind profilers' measurements (Spectral consistency, Beam agreement, Vertical coherence, Signal-to-noise thresholds)
- only quality-checked measurements a retained (logs count retained vs removed measurements).
  
7.4 Parallel extraction
Files are split into batches and computed with foreach/doParallel.
A global filtered dataset is produced and sorted.

7.5 Cleaning and physical variable computation
Wind speed = sqrt(u² + v²)
Wind direction = atan2(u, v)
Wind power density = ½ · r · U³
Turbine flags (cut-in, cut-out limits) are added.

7.6 Filtering steps
The script logs the impact of each filtering step at each height and station.

7.7 Aggregation workflow
– Hourly: statistical summaries including SD, turbulence intensity, wind run
– Daily: requires 24 hourly values
– Monthly: averages and extrema on daily values
– Yearly: averages and extrema on monthly values

7.8 NetCDF export
For each period (hourly/daily/monthly/yearly) and station:
- Long-format conversion
- Creation of NetCDF variables (values, timestamps, heights)
- Addition of metadata and CF-compliant attributes

An example of the daily wind data obtained with this data processing scheme is shown below:

<img width="627" height="555" alt="Screenshot 2025-11-27 alle 13 01 47" src="https://github.com/user-attachments/assets/f0aa42bb-3dcf-416f-a558-c8489207b10a" />

Figure 3: Daily Average Wind Direction Before vs. After QC – Station WMO 01018.


7.9 ADDITIONAL RECOMMENDATIONS

---
## 8. Automatic Tests

---

## 9. Known Limitations

___

### Current Limitations
The script in not made yet to download only the fraction of data added to the latest version, but it downloads all the data available in the CEDA archive

9.1 **Database Dependency**
    - None currently.

9.2 **Memory Usage**
   - Description: Parallel processing of large NetCDF files can consume significant RAM.
   - Workaround: Adjust the number of parallel workers if memory is constrained.

### Known Issues

---

## 10. Documentation Summary

### Available Documentation

- **README.md** - Quick start and overview.
- **SoftwareUserGuide.md** - This document.

### Code Documentation

- 

---

## 11. Support and Contact

### Reporting Issues

To report bugs, please contact the development team or open an issue in the repository.

### Contributing

Contributions are welcome. Please ensure tests pass before submitting changes.

**Maintainer(s):**

- Fabio Madonna

---

## 12. Deliverable Compliance

- [ ] Software placed in public repository
- [x] Complete source code included
- [x] Documentation included in repository
- [x] Installation instructions provided
- [x] Snapshot of test output provided
- [x] License specified
- [x] Software includes version number/release tag
- [x] README.md with quick start instructions
- [x] Example datasets or data access instructions provided

---

**Document Version:** 1.0.0
**Date:** 2025-11-24

