#!/bin/bash
###############################################################################
# Project:  E-PROFILE Automated Downloader
# File:     eprofile.sh
# Author:   Emanuele Tramutola
# Version:  1.0.0
# Date:     2025-01-01
#
# Description:
#   This script automates:
#     - Retrieval of CEDA trust roots via online_ca_client
#     - Automatic certificate generation for secure CEDA downloads
#     - Authenticated recursive download of wind profiler data from CEDA DAP
#
# Features:
#   - Checks and creates required directories
#   - Validates path transitions with error handling
#   - Automatically clones the online_ca_client repo if missing
#   - Produces a `creds.pem` certificate for wget authentication
#   - Downloads data recursively while preserving directory structure
#
# Requirements:
#   - bash >= 4
#   - git
#   - wget with SSL support
#   - Dependencies required by online_ca_client (openssl, python)
#
# Usage:
#   1. Edit the path variables in the configuration block
#   2. Run:  bash download_eprofile_data.sh
#
# Disclaimer:
#   Provided "as is". Validate permissions before running on production systems.
#
###############################################################################

# -------------------------------
# Set your paths here
# -------------------------------

# Directory where the source code will be cloned
SOURCE_DIR="src/eprofile/ceda_pydap_cert_code"   # <-- set your path

# Directory where trust roots will be stored
TRUSTROOTS_DIR="src/eprofile/trustroots"         # <-- set your path

# Base URL of the data to download
DATA_URL="https://dap.ceda.ac.uk/badc/ukmo-metdb/data/winpro/"

# Directory where downloaded data will be saved
DOWNLOAD_DIR="Data/eprofile"                     # <-- set your path

# Directory where your certificate will be saved
CERTIFICATE_DIR="."                              # <-- set your path

# -------------------------------
# Script execution
# -------------------------------

# Create the source directory if it doesn't exist
mkdir -p $SOURCE_DIR || { echo "Failed to create directory $SOURCE_DIR"; exit 1; }

# Change to the source directory
cd $SOURCE_DIR || { echo "Failed to change to directory $SOURCE_DIR"; exit 1; }

chmod +x onlineca-get-trustroots.sh
chmod +x onlineca-get-cert.sh

# Clone the online_ca_client repository if not already present
if [ ! -d "online_ca_client" ]; then
    git clone https://github.com/cedadev/online_ca_client || { echo "Failed to clone repository"; exit 1; }
fi

# Change to the appropriate directory to run the scripts
cd online_ca_client/contrail/security/onlineca/client/sh/ || { echo "Failed to change to directory"; exit 1; }

# Run the script to get trust roots
./onlineca-get-trustroots.sh -U https://slcs.ceda.ac.uk/onlineca/trustroots/ -c $TRUSTROOTS_DIR -b || { echo "Failed to get trust roots"; exit 1; }

# Run the script to get a certificate
./onlineca-get-cert.sh -U https://slcs.ceda.ac.uk/onlineca/certificate/ -c $TRUSTROOTS_DIR -l fkarimian -o $CERTIFICATE_DIR/creds.pem || { echo "Failed to get certificate"; exit 1; }

# Create the download directory if it doesn't exist
mkdir -p $DOWNLOAD_DIR || { echo "Failed to create download directory $DOWNLOAD_DIR"; exit 1; }

# Download the files to the specified directory
wget --certificate=$CERTIFICATE_DIR/creds.pem -r -P $DOWNLOAD_DIR -np -nH --cut-dirs=3 -R index.html $DATA_URL
