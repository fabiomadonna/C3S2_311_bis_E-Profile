###############################################################################
# Project: E-PROFILE Wind Profiler Data Processing script
# File:    eprofile.R
# Author:  Faezeh Karimian Saracks, Fabio Madonna.
# Version: 1.0.0
# Date:    2025-01-01
#
# Description:
#   This script implements a full end-to-end data processing pipeline for
#   E-PROFILE wind profiler data. It includes:
#
#     1. Metadata extraction from raw CSV files
#     2. Parallel file parsing and quality-controlled data extraction
#     3. Cleaning and computation of derived wind variables
#     4. Stepwise filtering with detailed logging
#     5. Multi-resolution temporal aggregation (hourly → daily → monthly → yearly)
#     6. Integration of static metadata from external spreadsheets
#     7. NetCDF export using CF-1.8 compliant structure
#     8. Runtime measurement and I/O handling
#
# Key Features:
#   - Fully parallelized sections for high-volume datasets
#   - Robust error handling in file processing
#   - Yamartino wind direction standard deviation
#   - Automated wind-energy metrics (WPD, turbulence, stress)
#   - Compatibility with wind turbine operational ranges
#   - Generation of station-level NetCDF files for each temporal resolution
#
# Requirements:
#   R >= 4.2.0
#   Packages: readr, dplyr, parallel, lubridate, foreach, doParallel,
#             fasttime, data.table, readxl, magrittr, ncdf4, zoo, writexl, progress
#
# Usage:
#   - Set working directory and user paths in the section below.
#   - Place raw CSV files inside the input folder (data_dir).
#   - Run the script from start to end for a complete pipeline execution.
#
# Notes:
#   - All time variables are normalized to UTC.
#   - NetCDF time units follow CF conventions and vary by aggregation level.
#   - Output files include filtering logs, combined datasets, and NetCDF products.
#
# Disclaimer:
#   This script is provided "as is". Adapt it as needed for your workflow.
#
###############################################################################

# Set your working directory here
setwd("/data")  # <-- set your path

## RUNTIME TIMER 
start_time <- Sys.time()
start_cpu  <- proc.time()

# Packages
required_packages <- c(
  "readr","dplyr","parallel","lubridate","foreach","doParallel",
  "fasttime","data.table","readxl","magrittr","ncdf4","zoo","writexl","progress", "arrow"
)
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE)) {
    message(paste(" Installing missing package:", pkg))
    install.packages(pkg, dependencies = TRUE)
    library(pkg, character.only = TRUE)
  } else {
    suppressPackageStartupMessages(library(pkg, character.only = TRUE))
  }
}

# --- LEGGI L'ANNO DA RIGA DI COMANDO (BASH) ---
args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0) {
  stop("❌ Errore: Devi specificare l'anno come argomento da riga di comando! Es: Rscript eprofile.R 2009")
}
year_val <- args[1]
message("📅 Anno ricevuto da Bash: ", year_val)

# USER PATHS (set your paths)
data_dir           <- paste0("/data/winpro/", year_val, "/")
output_file_prefix <- paste0("testttttt_complete_", year_val)
log_dir            <- file.path("/data/winpro", year_val, "logs")
nc_out_dir         <- file.path("/data/winpro", year_val)

# Creazione cartelle
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(nc_out_dir, recursive = TRUE, showWarnings = FALSE)

# 1) METADATA EXTRACTION 
extract_all_metadata <- function(file) {

  # Legge il file come testo (molto più veloce)
  lines <- readLines(file, warn = FALSE)

  metadata_list <- list()
  
  start_rows <- grep("^Conventions", lines)
  end_rows   <- grep("^number_of_levels", lines)

  if (length(start_rows) == 0) return(data.frame())

  for (i in seq_along(start_rows)) {

    start_row <- start_rows[i]
    end_row   <- ifelse(i <= length(end_rows), end_rows[i], length(lines))

    block <- lines[start_row:end_row]

    # Estrazione campi con grep (zero parsing pesante)
    get_val <- function(pattern, col = 3) {
      row <- grep(paste0("^", pattern), block, value = TRUE)
      if (length(row) == 0) return(NA)
      parts <- strsplit(row[1], ",")[[1]]
      if (length(parts) >= col) return(parts[col])
      return(NA)
    }

    station <- get_val("observation_station", 3)

    if (!is.na(station)) {
      metadata_list[[length(metadata_list) + 1]] <- data.frame(
        observation_station          = station,
        date_valid                   = get_val("date_valid", 3),
        location_latitude            = as.numeric(get_val("location", 3)),
        location_longitude           = as.numeric(get_val("location", 4)),
        height                       = as.numeric(get_val("height", 3)),
        station_type                 = as.numeric(get_val("station_type", 4)),
        type_of_measuring_equipment  = as.numeric(get_val("type_of_measuring_equipment", 4)),
        type_of_antenna              = as.numeric(get_val("type_of_antenna", 4)),
        mean_speed_estimation        = as.numeric(get_val("mean_speed_estimation", 4)),
        wind_computation_enhancement = as.numeric(get_val("wind_computation_enhancement", 4)),
        stringsAsFactors = FALSE
      )
    }
  }

  do.call(rbind, metadata_list)
}

run_metadata_extraction <- function(csv_files) {
  cl <- parallel::makeCluster(max(1, parallel::detectCores()-1))
  on.exit(parallel::stopCluster(cl), add = TRUE)
  parallel::clusterExport(cl, varlist = c("extract_all_metadata"), envir = environment())
  parallel::clusterEvalQ(cl, { library(readr); library(dplyr); library(magrittr) })
  metadata_list <- parallel::parLapply(cl, csv_files, extract_all_metadata)
  metadata_df   <- dplyr::bind_rows(metadata_list) %>%
    dplyr::group_by(observation_station, location_latitude, location_longitude, height,
                    station_type, type_of_measuring_equipment, type_of_antenna,
                    mean_speed_estimation, wind_computation_enhancement) %>%
    dplyr::summarize(date_valid = paste(unique(date_valid), collapse = ","), .groups = 'drop') %>%
    dplyr::mutate(id = dplyr::row_number()) %>%
    dplyr::select(id, dplyr::everything())
  metadata_df
}

#  2) FILE PROCESSING 
process_file <- function(file) {
  lines <- readLines(file, warn = FALSE)
  st_i  <- grep("^observation_station", lines)
  dv_i  <- grep("^date_valid", lines)
  station_vec    <- sub(".*observation_station,G,(WMO \\d+).*", "\\1", lines[st_i])
  date_valid_vec <- sub(".*date_valid,G,(.*)", "\\1", lines[dv_i])
  
  starts <- grep("^data$", lines)
  ends   <- grep("^end data$", lines)
  if (length(starts) != length(ends) || length(starts) == 0) {
    return(list(data = data.table(), log = data.table()))
  }
  
  log_list <- vector("list", length(starts))
  out_list <- vector("list", length(starts))
  
  for (i in seq_along(starts)) {
    block <- lines[(starts[i] + 1):(ends[i] - 1)]
    raw   <- data.table::fread(text = paste(block, collapse = "\n"), header = TRUE)
    
    sel <- raw[, .(
      station    = station_vec[i],
      Date_valid = date_valid_vec[i],
      X1 = get(names(raw)[1]),
      X4 = get(names(raw)[4]),
      X6 = get(names(raw)[6]),
      X7 = get(names(raw)[7])
    )]
    
    n_before <- nrow(sel)
    filtered <- sel[X4 == 0]
    n_after  <- nrow(filtered)
    
    log_list[[i]] <- data.table(
      station     = station_vec[i],
      Date_valid  = date_valid_vec[i],
      n_before    = n_before,
      n_after     = n_after,
      pct_retained= round(100 * n_after / max(1,n_before), 2)
    )
    
    data.table::setorder(filtered, station, Date_valid, X1)
    out_list[[i]] <- filtered
  }
  
  list(
    data = data.table::rbindlist(out_list, use.names = TRUE, fill = TRUE),
    log  = data.table::rbindlist(log_list, use.names = TRUE, fill = TRUE)
  )
}

#  3) PARALLEL EXTRACTION 
run_data_extraction <- function(csv_files) {

  library(doParallel)
  library(foreach)
  library(data.table)

  ncores <- min(64, parallel::detectCores() - 1)
  registerDoParallel(ncores)
  on.exit(stopImplicitCluster(), add = TRUE)

  message(sprintf(
    "⏳ Processing %d files using %d cores ...",
    length(csv_files), ncores
  ))

  nchunks <- ncores
  chunked <- split(
    csv_files,
    ceiling(seq_along(csv_files) /
              ceiling(length(csv_files) / nchunks))
  )

  res_list <- foreach(
    batch = chunked,
    .packages = "data.table",
    .export   = "process_file",
    .errorhandling = "pass"
  ) %dopar% {

    outs <- lapply(batch, function(f) {
      tryCatch(
        process_file(f),
        error = function(e) list(data = data.table(), log = data.table())
      )
    })

    list(
      data = data.table::rbindlist(lapply(outs, `[[`, "data"),
                                   use.names = TRUE, fill = TRUE),
      log  = data.table::rbindlist(lapply(outs, `[[`, "log"),
                                   use.names = TRUE, fill = TRUE)
    )
  }

  message("✅ Parallel extraction complete. Combining results...")

  combined_data <- data.table::rbindlist(
    lapply(res_list, `[[`, "data"),
    use.names = TRUE, fill = TRUE
  )

  combined_log <- data.table::rbindlist(
    lapply(res_list, `[[`, "log"),
    use.names = TRUE, fill = TRUE
  )

  combined_data <- combined_data %>%
    dplyr::distinct(station, Date_valid, X1, .keep_all = TRUE) %>%
    dplyr::arrange(station, fasttime::fastPOSIXct(Date_valid))

  fwrite(combined_log, file.path(data_dir, "filter_log_X4_check.csv"))

  data.table::as.data.table(combined_data)
}

#  4) MASTER 
run_all_processing <- function(data_dir, output_file_prefix = "testttttt_complete") {
  csv_files <- list.files(data_dir, pattern = "\\.csv$", full.names = TRUE)
  message("Estrazione dati...")
  combined_data <- run_data_extraction(csv_files)
  combined_data <- combined_data %>%
    dplyr::distinct(station, Date_valid, X1, .keep_all = TRUE) %>%
    dplyr::arrange(station, fasttime::fastPOSIXct(Date_valid))
  combined_output_file <- file.path(data_dir, paste0(output_file_prefix, "_combined_data.csv"))
  data.table::fwrite(combined_data, combined_output_file)
  message("✅ File combinato salvato in: ", combined_output_file)
  combined_data
}

#  5) RUN & READ BACK 
all_csv_files <- list.files(
  data_dir,
  pattern = "\\.csv$",
  full.names = TRUE
)

file_dates <- as.Date(
  sub(".*_(\\d{8})\\.csv$", "\\1", basename(all_csv_files)),
  format = "%Y%m%d"
)

years <- sort(unique(lubridate::year(file_dates)))
message("📅 Years detected in input files: ", paste(years, collapse = ", "))

combined_data_all_years <- data.table()

for (yy in years) {

  message("🗓 Processing year: ", yy)

  files_year <- all_csv_files[
    lubridate::year(file_dates) == yy
  ]

  if (length(files_year) == 0) {
    message("⚠ No files found for year ", yy)
    next
  }

  combined_year <- run_data_extraction(files_year)

  combined_year <- combined_year %>%
    dplyr::distinct(station, Date_valid, X1, .keep_all = TRUE) %>%
    dplyr::arrange(station, fasttime::fastPOSIXct(Date_valid))

  combined_data_all_years <- data.table::rbindlist(
    list(combined_data_all_years, combined_year),
    use.names = TRUE,
    fill = TRUE
  )
}

combined_data <- combined_data_all_years
rm(combined_data_all_years)


#  6) CLEAN + WIND METRICS 
turbine_cut_in   <- 3.5   # m/s
turbine_cut_out  <- 25.0  # m/s
air_density <- 1.225  # kg/m³

data.table::setnames(
  combined_data,
  old = c("station","Date_valid","X1","X6","X7"),
  new = c("Station","Date","Height","u","v")
)

combined_data[, datetime := as.POSIXct(
  fasttime::fastPOSIXct(Date, required.components = 5),
  tz = "UTC"
)]

combined_data[, c("u","v") := lapply(.SD, dplyr::na_if, -9999999), .SDcols = c("u","v")]
combined_data <- combined_data[!is.na(u) & !is.na(v)]
combined_data[, observation_id := .I]

calculate_wind_speed     <- function(u, v) sqrt(u^2 + v^2)
yamartino_sd <- function(directions) {
  n <- length(directions); if (n <= 1) return(NA)
  rad_directions <- directions * pi / 180
  sin_sum <- sum(sin(rad_directions)); cos_sum <- sum(cos(rad_directions))
  mean_dir <- atan2(sin_sum / n, cos_sum / n)
  mean_dir <- ifelse(mean_dir < 0, mean_dir + 2*pi, mean_dir)
  mean_dir <- mean_dir * 180/pi
  diff_rad <- rad_directions - mean_dir * pi / 180
  sin_diff_sum <- sum(sin(diff_rad)); cos_diff_sum <- sum(cos(diff_rad))
  val <- 1 - (cos_diff_sum / n)^2 - (sin_diff_sum / n)^2
  val <- pmax(val, 0)
  epsilon <- sqrt(val)
  asin(epsilon) * 180 / pi * sqrt(2 * (1 - epsilon^2))
}
calculate_wind_run <- function(wind_speeds, dt_minutes) {
  sum(wind_speeds, na.rm = TRUE) * (dt_minutes / 60)
}

calculate_wind_direction <- function(u, v) {
  wd <- (180/pi) * atan2(u, v) + 180
  wd <- ifelse(wd < 0, wd + 360, wd)
  wd %% 360
}

combined_data[, wind_speed     := calculate_wind_speed(u, v)]
combined_data[, wind_direction := calculate_wind_direction(u, v)]
combined_data[, wind_power_density := 0.5 * air_density * (wind_speed^3)]

combined_data[, op_flag := wind_speed >= turbine_cut_in & wind_speed <= turbine_cut_out]

cat("✅ Wind power density added to dataset (mean WPD =",
    round(mean(combined_data$wind_power_density, na.rm = TRUE), 2), "W/m² )\n")

#  7) STEPWISE FILTER + LOG 
log_filter_counts <- function(step_name, before_dt, after_dt, log_list) {
  counts_before <- before_dt[, .N, by = .(Station, Height)]
  counts_after  <- after_dt[, .N, by = .(Station, Height)]
  log <- merge(counts_before, counts_after, by = c("Station","Height"), all.x = TRUE, suffixes = c("_before","_after"))
  log[is.na(N_after), N_after := 0]
  log[, filtered := N_before - N_after]
  log[, step := step_name]
  data.table::setcolorder(log, c("step","Station","Height","N_before","N_after","filtered"))
  log_list[[length(log_list) + 1]] <- log
  log_list
}

log_steps  <- list()
data_step_0 <- data.table::copy(combined_data)

data_step_1 <- data_step_0[
  wind_speed     >= 0 & wind_speed <= 100 &
    wind_direction >= 0 & wind_direction < 360
]
log_steps <- log_filter_counts("Implausible-value filter", data_step_0, data_step_1, log_steps)

combined_data <- data_step_1
data.table::setorder(combined_data, Station, datetime, Height)

filter_log <- data.table::rbindlist(log_steps, use.names = TRUE, fill = TRUE)
data.table::fwrite(filter_log, file.path(log_dir, paste0(output_file_prefix, "filtering_log.csv")))
cat("✅ Filtering log saved to:", file.path(log_dir, paste0(output_file_prefix, "filtering_log.csv")), "\n")


# ============================================================
#  8) METADATA LOAD (CON FIX COORDINATE E CALCOLO DEM ASL VIA API)
# ============================================================

message("📡 Extracting metadata from CSV files...")

csv_files <- list.files(
  data_dir,
  pattern = "\\.csv$",
  full.names = TRUE
)

metadata_raw <- run_metadata_extraction(csv_files)

# ------------------------------------------------------------
# PULIZIA + UN SOLO RECORD PER STAZIONE
# ------------------------------------------------------------
metadata_raw <- data.table::as.data.table(metadata_raw)

static_metadata <- metadata_raw[
  , .(
    location_latitude  = unique(na.omit(location_latitude))[1],
    location_longitude = unique(na.omit(location_longitude))[1],
    height             = unique(na.omit(height))[1]
  ),
  by = observation_station
]
static_metadata[, observation_station := trimws(observation_station)]

# ------------------------------------------------------------
# HARDCODED FIX PER STAZIONI CON COORDINATE CORROTTE O MISSING
# ------------------------------------------------------------
is_coord_invalid <- function(lat, lon) {
  is.na(lat) | is.na(lon) | 
  lat < -90 | lat > 90 | 
  lon < -180 | lon > 180 | 
  abs(lat) > 9000 | abs(lon) > 9000 |
  (lat == 0 & lon == 0)
}

static_metadata[observation_station == "WMO 10135" & is_coord_invalid(location_latitude, location_longitude), `:=`(location_latitude = 53.7776, location_longitude = 28.667632)]
static_metadata[observation_station == "WMO 10266" & is_coord_invalid(location_latitude, location_longitude), `:=`(location_latitude = 53.31033, location_longitude = 11.836671)]
static_metadata[observation_station == "WMO 10394" & is_coord_invalid(location_latitude, location_longitude), `:=`(location_latitude = 52.209314, location_longitude = 14.128573)]
static_metadata[observation_station == "WMO 03962" & is_coord_invalid(location_latitude, location_longitude), `:=`(location_latitude = 52.7, location_longitude = -8.92)]
static_metadata[observation_station == "WMO 03969" & is_coord_invalid(location_latitude, location_longitude), `:=`(location_latitude = 53.43, location_longitude = -6.24)]

# ------------------------------------------------------------
# CALCOLO HEIGHT ABOVE SEA LEVEL DA DEM (VIA OPEN-METEO API)
# ------------------------------------------------------------
message("🏔 Calculating station height above sea level using Open-Meteo DEM API...")

get_elevation_from_api <- function(lat, lon) {
  if (is.na(lat) || is.na(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) {
    return(NA_real_)
  }
  
  url_str <- sprintf("https://api.open-meteo.com/v1/elevation?latitude=%f&longitude=%f", lat, lon)
  
  res <- tryCatch({
    options(timeout = 10) 
    con <- url(url_str)
    lines <- readLines(con, warn = FALSE)
    close(con)
    
    match_idx <- regexpr('"elevation":\\s*\\[\\s*([0-9.-]+)\\s*\\]', lines)
    if (match_idx > 0) {
      match_str <- regmatches(lines, match_idx)
      num_str <- gsub('[^0-9.-]', '', match_str)
      return(as.numeric(num_str))
    } else {
      return(NA_real_)
    }
  }, error = function(e) {
    return(NA_real_)
  })
  return(res)
}

static_metadata[, height_of_station_above_sea_level := mapply(
  get_elevation_from_api, 
  location_latitude, 
  location_longitude
)]

message("✅ DEM height calculation completed successfully.")

# ------------------------------------------------------------
# CHECK QUALITÀ CORRETTO
# ------------------------------------------------------------
missing_lat <- sum(is.na(static_metadata$location_latitude))
missing_lon <- sum(is.na(static_metadata$location_longitude))

if (missing_lat > 0 || missing_lon > 0) {
  warning(paste0("⚠ Missing coordinates after fix: lat=", missing_lat, " lon=", missing_lon))
}

message("✅ Metadata ready: ", nrow(static_metadata), " stations found")


#  9) AGGREGATION (con progress bar)
add_report_id <- function(df) {
  df[, report_id := .GRP, by = .(Station, Height, interval)]
  df
}

aggregate_wind_data <- function(data, interval, interval_minutes) {
  data <- data.table::copy(data)
  data[, interval := lubridate::floor_date(datetime, interval)]
  
  stations <- unique(data$Station)
  ncores   <- min(max(1, parallel::detectCores()-1), length(stations))
  doParallel::registerDoParallel(ncores); on.exit(stopImplicitCluster(), add = TRUE)
  
  stn_chunks <- split(stations, ceiling(seq_along(stations) / ceiling(length(stations) / ncores)))
  
  total_stations <- length(stations)
  pb <- txtProgressBar(min = 0, max = total_stations, style = 3)
  progress_counter <- 0
  
  result_list <- foreach::foreach(
    stns = stn_chunks,
    .packages = c("data.table", "stats"),
    .export   = c("yamartino_sd","calculate_wind_run","calculate_wind_direction","air_density"),
    .errorhandling = "pass"
  ) %dopar% {
    outs <- lapply(stns, function(stn) {
      dt <- data[Station == stn]
      if (!nrow(dt)) return(NULL)
      
      dt_med_minutes <- suppressWarnings(stats::median(as.numeric(diff(dt$datetime))/60, na.rm = TRUE))
      if (!is.finite(dt_med_minutes) || dt_med_minutes <= 0) dt_med_minutes <- interval_minutes
      
      ans <- dt[, .(
        num_obs = .N,
        avg_wind_speed = mean(wind_speed, na.rm = TRUE),
        max_wind_speed = max(wind_speed, na.rm = TRUE),
        min_wind_speed = min(wind_speed, na.rm = TRUE),
        sd_wind_speed  = if (.N > 1) sd(wind_speed, na.rm = TRUE) else NA_real_,
        avg_wind_direction = if (.N > 1) calculate_wind_direction(mean(u, na.rm = TRUE), mean(v, na.rm = TRUE)) else NA_real_,
        sd_wind_direction  = if (.N > 1) yamartino_sd(wind_direction) else NA_real_,
        wind_run = calculate_wind_run(wind_speed, dt_med_minutes),
        avg_wpd  = mean(wind_power_density, na.rm = TRUE),
        turbulence_intensity = if (.N > 1 && mean(wind_speed, na.rm = TRUE) > 0)
          sd(wind_speed, na.rm = TRUE) / mean(wind_speed, na.rm = TRUE) else NA_real_,
        wind_stress = air_density * (mean(wind_speed, na.rm = TRUE)^2)
      ), by = .(Station, Height, interval)]
      
      progress_counter <<- progress_counter + 1
      setTxtProgressBar(pb, progress_counter)
      
      ans
    })
    data.table::rbindlist(outs, use.names = TRUE, fill = TRUE)
  }
  
  close(pb)
  
  result <- data.table::rbindlist(result_list, use.names = TRUE, fill = TRUE)
  result <- add_report_id(result)
  result[order(Station, interval, Height)]
}

hourly_data <- aggregate_wind_data(combined_data, "hour", 60)

daily_data <- hourly_data[, .(
  num_obs = .N,
  avg_wind_speed = if(.N>0) mean(avg_wind_speed, na.rm=TRUE) else NA_real_,
  max_wind_speed = if(.N>0) max(max_wind_speed, na.rm=TRUE) else NA_real_,
  min_wind_speed = if(.N>0) min(min_wind_speed, na.rm=TRUE) else NA_real_,
  sd_wind_speed  = if(.N>1) sd(avg_wind_speed, na.rm=TRUE) else NA_real_,
  avg_wind_direction = if(.N>0) mean(avg_wind_direction, na.rm=TRUE) else NA_real_,
  sd_wind_direction  = if(.N>1) yamartino_sd(avg_wind_direction) else NA_real_,
  wind_run = if(.N>0) sum(wind_run, na.rm=TRUE) else NA_real_,
  avg_wpd  = if(.N>0) mean(avg_wpd, na.rm=TRUE) else NA_real_,
  turbulence_intensity = mean(turbulence_intensity, na.rm = TRUE),
  wind_stress = mean(wind_stress, na.rm = TRUE)
), by = .(Station, Height, day = as.Date(interval))][num_obs == 24]

daily_summary <- daily_data[, .(total_days = .N), by = .(Station, Height)]
daily_summary[, threshold := 0.75 * total_days]
daily_data_filtered <- daily_data[daily_summary, on = .(Station, Height), nomatch = 0][ num_obs >= threshold]

monthly_data <- daily_data_filtered[, .(
  avg_wind_speed     = mean(avg_wind_speed, na.rm = TRUE),
  max_wind_speed     = max(max_wind_speed, na.rm = TRUE),
  min_wind_speed     = min(min_wind_speed, na.rm = TRUE),
  sd_wind_speed      = sd(avg_wind_speed, na.rm = TRUE),
  avg_wind_direction = mean(avg_wind_direction, na.rm = TRUE),
  sd_wind_direction  = if(.N>1) yamartino_sd(avg_wind_direction) else NA_real_,
  wind_run           = sum(wind_run, na.rm = TRUE),
  avg_wpd            = mean(avg_wpd, na.rm = TRUE),
  turbulence_intensity = mean(turbulence_intensity, na.rm = TRUE),
  wind_stress        = mean(wind_stress, na.rm = TRUE)
), by = .(Station, Height, month = lubridate::floor_date(day, "month"))]

yearly_data <- monthly_data[, .(
  avg_wind_speed     = mean(avg_wind_speed, na.rm = TRUE),
  max_wind_speed     = max(max_wind_speed, na.rm = TRUE),
  min_wind_speed     = min(min_wind_speed, na.rm = TRUE),
  sd_wind_speed      = sd(avg_wind_speed, na.rm = TRUE),
  avg_wind_direction = mean(avg_wind_direction, na.rm = TRUE),
  sd_wind_direction  = if(.N>1) yamartino_sd(avg_wind_direction) else NA_real_,
  wind_run           = sum(wind_run, na.rm = TRUE),
  avg_wpd            = mean(avg_wpd, na.rm = TRUE),
  turbulence_intensity = mean(turbulence_intensity, na.rm = TRUE),
  wind_stress        = mean(wind_stress, na.rm = TRUE)
), by = .(Station, Height, year = lubridate::year(month))]

#  10) SAVE CSVs 
data.table::fwrite(hourly_data,  file.path(nc_out_dir, "hourly_wind_data.csv"))
data.table::fwrite(daily_data,   file.path(nc_out_dir, "daily_wind_data.csv"))
data.table::fwrite(monthly_data, file.path(nc_out_dir, "monthly_wind_data.csv"))
data.table::fwrite(yearly_data,  file.path(nc_out_dir, "yearly_wind_data.csv"))
cat("Aggregated wind data saved as CSV files in:", nc_out_dir, "\n")


# ==============================================================================
#  11) NORMALIZE TIME COLUMN NAMES (FIX ROBUSTO PER -Inf, NA E STRINGHE CORROTTE)
# ==============================================================================
for (name in c("hourly_data","daily_data","monthly_data","yearly_data")) {
  dt <- get(name)

  if (nrow(dt) == 0) next

  for (col in names(dt)) {
    if (is.numeric(dt[[col]])) {
      dt[get(col) == -Inf, (col) := NA_real_]
      dt[get(col) == Inf,  (col) := NA_real_]
    }
  }

  if ("interval" %in% names(dt)) {
    data.table::setnames(dt, "interval", "time")
    dt[, time := lubridate::as_datetime(time, tz = "UTC")]
  }

  else if ("day" %in% names(dt)) {
    data.table::setnames(dt, "day", "time")
    dt[, time := lubridate::as_datetime(time, tz = "UTC")]
  }

  else if ("month" %in% names(dt)) {
    data.table::setnames(dt, "month", "time")
    dt[, time := lubridate::as_datetime(time, tz = "UTC")]
  }

  else if ("year" %in% names(dt)) {
    data.table::setnames(dt, "year", "time")
    dt[!is.na(time) & as.character(time) != "NA", time_clean := paste0(time, "-01-01 00:00:00")]
    dt[, time := lubridate::as_datetime(time_clean, tz = "UTC")]
    dt[, time_clean := NULL]
  }

  dt <- dt[!is.na(time)]

  if (!"wind_run" %in% names(dt)) dt[, wind_run := NA_real_]

  assign(name, dt, inherits = TRUE)
}

#  12) NetCDF TIME UNITS MAP 
time_units_map <- list(
  hourly  = "hours since 1970-01-01 00:00:00",
  daily   = "hours since 1970-01-01 00:00:00",
  monthly = "hours since 1970-01-01 00:00:00",
  yearly  = "hours since 1970-01-01 00:00:00"
)

report_duration_map <- list(hourly = 9L, daily = 13L, monthly = 14L, yearly = 18L)

units_map <- list(
  avg_wind_speed       = 731,  # m/s
  max_wind_speed       = 731,
  min_wind_speed       = 731,
  sd_wind_speed        = 731,
  avg_wind_direction   = 110,  # degree
  sd_wind_direction    = 110,  # stdev
  wind_run             = 1,    # W/m2
  avg_wpd              = 811,  # m/s
  turbulence_intensity = 1016, # adimensional
  wind_stress          = 795   # N m-2
)

value_significance_map <- list(
  avg_wind_speed       = 2,
  max_wind_speed       = 0,
  min_wind_speed       = 1,
  sd_wind_speed        = 7,
  avg_wind_direction   = 2,
  sd_wind_direction    = 7,
  wind_run             = 13,
  avg_wpd              = 2,
  turbulence_intensity = 7,
  wind_stress          = 2
)


# 13) NetCDF CREATION 
create_ncdf <- function(df, output_file, meta, time_units, aggregation_level) {
  library(data.table)
  library(ncdf4)
  library(arrow)

  df <- as.data.table(df)
  station_id <- unique(df$Station)

  df[, report_key := paste(Station, time, sep = "_")]
  df[, observation_key := paste(Station, time, Height, sep = "_")]

  df[, report_id := as.integer(factor(report_key))]
  df[, observation_id := as.integer(factor(observation_key))]

  obs_vars <- intersect(c(
    "avg_wind_speed","max_wind_speed","min_wind_speed",
    "sd_wind_speed","avg_wind_direction","sd_wind_direction",
    "wind_run","avg_wpd","turbulence_intensity","wind_stress"
  ), names(df))
  
  df_long <- melt(
    df,
    id.vars = c("time","Station","Height","report_id","observation_id"),
    measure.vars = obs_vars,
    variable.name = "variable",
    value.name    = "observation_value"
  )[!is.na(observation_value)]

  df_long[, observed_variable := unname(c(
    avg_wind_speed        = 107L,
    max_wind_speed        = 107L,
    min_wind_speed        = 107L,
    sd_wind_speed         = 107L,
    avg_wind_direction    = 106L,
    sd_wind_direction     = 106L,
    wind_run              = 203L,
    avg_wpd               = 204L,
    turbulence_intensity  = 110L,
    wind_stress           = 205L
  )[variable])]
  
  df_long[, z_coordinate := Height]
  df_long[, z_coordinate_type := 0L]
  df_long[, report_timestamp := as.numeric(difftime(
    time,
    as.POSIXct("1970-01-01 00:00:00", tz="UTC"),
    units = "hours"
  ))]

  df_long[, index := .I]
  dim_index <- ncdim_def("index", "", df_long$index, create_dimvar = TRUE)

  vars <- list(
    ncvar_def("observation_value", "1", list(dim_index), prec="single", missval=NaN),
    ncvar_def("observed_variable", "1", list(dim_index), prec="single"),
    ncvar_def("z_coordinate", "m", list(dim_index), prec="single"),
    ncvar_def("z_coordinate_type", "1", list(dim_index), prec="short"),
    ncvar_def("report_timestamp", "hours since 1970-01-01 00:00:00", list(dim_index), prec="double"),
    ncvar_def("observation_id", "1", list(dim_index), prec="integer"),
    ncvar_def("report_id", "1", list(dim_index), prec="integer"),
    ncvar_def("report_duration", "1", list(dim_index), prec="integer"),
    ncvar_def("value_significance", "1", list(dim_index), prec="integer"),
    ncvar_def("units", "1", list(dim_index), prec="integer")
  )

  if (file.exists(output_file)) file.remove(output_file)
  nc <- nc_create(output_file, vars)

  df_long[, report_duration := report_duration_map[[as.character(aggregation_level)]]]

  if (is.null(df_long$report_duration)) {
    stop(paste("Invalid aggregation_level:", aggregation_level))
  }

  df_long[, value_significance := as.integer(unlist(value_significance_map[variable]))]
  df_long[, units := as.integer(unlist(units_map[variable]))]

  ncvar_put(nc,"observation_value", df_long$observation_value)
  ncvar_put(nc,"observed_variable", df_long$observed_variable)
  ncvar_put(nc,"z_coordinate", df_long$z_coordinate)
  ncvar_put(nc,"z_coordinate_type", df_long$z_coordinate_type)
  ncvar_put(nc,"report_timestamp", df_long$report_timestamp)
  ncvar_put(nc,"observation_id", df_long$observation_id)
  ncvar_put(nc,"report_id", df_long$report_id)
  ncvar_put(nc,"report_duration", df_long$report_duration)
  ncvar_put(nc,"value_significance", df_long$value_significance)
  ncvar_put(nc,"units", df_long$units)

  ncdf4::ncatt_put(nc,"report_timestamp","units", time_units)
  ncdf4::ncatt_put(nc,"report_timestamp","calendar","gregorian")
  ncdf4::ncatt_put(nc,0,"title","E-PROFILE time-aggregated wind profilers' data")
  ncdf4::ncatt_put(nc,0,"institution","University of Salerno")
  ncdf4::ncatt_put(nc,0,"source","Wind profilers")
  ncdf4::ncatt_put(nc,0,"primary_id", station_id)

  lat <- meta$location_latitude[1]
  lon <- meta$location_longitude[1]
  alt <- meta$height_of_station_above_sea_level[1] # <-- MODIFICA AGGIUNTA QUI
  
  if (is.na(lat)) lat <- NA_real_
  if (is.na(lon)) lon <- NA_real_
  if (is.na(alt)) alt <- NA_real_                  # <-- MODIFICA AGGIUNTA QUI

  ncdf4::ncatt_put(nc,0,"station_elevation_above_sea_level_m", alt) # <-- MODIFICA AGGIUNTA QUI (Attributo NetCDF)

  df_parquet <- df_long[, .(
    primary_station_id          = as.character(Station),
    "latitude|station_configuration"  = as.numeric(lat), 
    "longitude|station_configuration" = as.numeric(lon), 
    "height_of_station_above_sea_level" = as.numeric(alt), # <-- MODIFICA AGGIUNTA QUI (Colonna Parquet)
    observation_value           = as.numeric(observation_value),
    observed_variable           = as.integer(observed_variable),
    z_coordinate                = as.numeric(z_coordinate),
    z_coordinate_type           = as.integer(z_coordinate_type),
    report_timestamp            = as.POSIXct(report_timestamp * 3600, origin = "1970-01-01", tz = "UTC"),
    observation_id              = as.character(observation_id),
    report_id                   = as.character(report_id),
    report_duration             = as.integer(report_duration),
    value_significance          = as.integer(value_significance),
    units                       = as.integer(units),
    observing_programme         = "27",
    report_meaning_of_timestamp = 3L,
    data_policy_licence         = 5L,
    quality_flag                = 0L
  )]

  df_parquet <- df_parquet[
    !is.na(`latitude|station_configuration`) & 
    !is.na(`longitude|station_configuration`) & 
    !is.na(observed_variable) & 
    !is.na(report_timestamp)
  ]

  parquet_file <- sub("\\.nc$", ".parquet", output_file)

  arrow::write_parquet(
    df_parquet, 
    parquet_file,
    use_deprecated_int96_timestamps = FALSE, 
    version = "2.6", 
    compression = "snappy"
  )

  nc_close(nc)
  message("Saved NetCDF: ", output_file)
  message("Saved Parquet (Fixed Metadata): ", parquet_file)
}

# ==============================================================================
#  15) WRITE NetCDF per STATION & LEVEL (CON FIX TRY-CATCH)
# ==============================================================================
output_prefix <- file.path(nc_out_dir, "wind_")

aggregated_data_list <- list(
  hourly  = hourly_data,
  daily   = daily_data,
  monthly = monthly_data,
  yearly  = yearly_data
)

for (level in names(aggregated_data_list)) {

  df_level <- aggregated_data_list[[level]]
  if (!nrow(df_level)) next

  for (stn in unique(df_level$Station)) {

    df_stn   <- df_level[Station == stn]
    meta_stn <- static_metadata[observation_station == stn]

    safe    <- gsub(" ","_", stn)
    outfile <- sprintf("%s%s_%s.nc", output_prefix, level, safe)

    tryCatch({
      create_ncdf(
        df_stn,
        outfile,
        meta_stn,
        time_units_map[[level]],
        level
      )
    }, error = function(e) {
      message(sprintf("⚠️ [SALTO STAZIONE] Errore nel formato timestamp/POSIXct per la stazione %s (%s): %s", stn, level, e$message))
    })
  }
}

# ==============================================================================
# 16) MERGE AND FORMAT PARQUET FILES BY YEAR (CON FIX PER FILE MANCANTI/CORROTTI)
# ==============================================================================
agg_levels <- c("hourly", "daily", "monthly", "yearly")

for (lvl in agg_levels) {

  parquet_files <- list.files(
    nc_out_dir,
    pattern = sprintf("wind_%s_.*\\.parquet$", lvl),
    full.names = TRUE
  )

  if (!length(parquet_files)) next

  message("Processing level: ", lvl)

  dt_list <- lapply(parquet_files, function(f) {
    tryCatch({
      df <- arrow::read_parquet(
        f,
        col_select = c(
          "primary_station_id",
          "latitude|station_configuration",
          "longitude|station_configuration",
          "height_of_station_above_sea_level",
          "observation_value",
          "observed_variable",
          "z_coordinate",
          "z_coordinate_type",
          "report_timestamp",
          "observation_id",
          "report_id",
          "report_duration",
          "value_significance",
          "units",
          "observing_programme",
          "report_meaning_of_timestamp",
          "data_policy_licence",
          "quality_flag"
        )
      )
      return(data.table::as.data.table(df))
    }, error = function(e) {
      message(sprintf("⚠️ [SALTO FILE PARQUET] Impossibile leggere o convertire il file %s: %s", basename(f), e$message))
      return(NULL)
    })
  })

  dt_list <- dt_list[!sapply(dt_list, is.null)]

  if (length(dt_list) == 0) {
    message("  ❌ Nessun dato valido rimasto per il livello: ", lvl)
    next
  }

  dt_year <- data.table::rbindlist(
    dt_list,
    use.names = TRUE,
    fill = TRUE
  )

  final_file <- file.path(
    nc_out_dir,
    sprintf("ALL_STATIONS_wind_%s.parquet", lvl)
  )

  arrow::write_parquet(
    dt_year,
    final_file,
    version = "2.6",
    compression = "snappy",
    use_deprecated_int96_timestamps = FALSE
  )

  rm(dt_list, dt_year)
  gc()

  message("  Saved: ", final_file)
}

## PRINT TOTAL RUNTIME
end_time <- Sys.time()
end_cpu  <- proc.time()

cat("Total wall time :", round(difftime(end_time, start_time, units = "secs"), 2), "seconds\n")
cat("Total CPU time  :", round((end_cpu - start_cpu)[["elapsed"]], 2), "seconds\n")
