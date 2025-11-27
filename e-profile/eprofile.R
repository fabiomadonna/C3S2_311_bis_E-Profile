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
#             fasttime, data.table, readxl, magrittr, ncdf4, zoo, writexl
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
setwd("E-PROFILE")  # <-- set your path

## RUNTIME TIMER 
start_time <- Sys.time()
start_cpu  <- proc.time()

# Packages
required_packages <- c(
  "readr","dplyr","parallel","lubridate","foreach","doParallel",
  "fasttime","data.table","readxl","magrittr","ncdf4","zoo","writexl"
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

# USER PATHS (set your paths)
data_dir           <- "Data_eprofile"       # <-- set your path for input/output CSVs
output_file_prefix <- "testttttt_complete"  # output file prefix
log_dir            <- "logs"                # <-- set your path for logs
nc_out_dir         <- "Data_eprofile"       # <-- set your path for NetCDF output
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(nc_out_dir, recursive = TRUE, showWarnings = FALSE)

# 1) METADATA EXTRACTION 
extract_all_metadata <- function(file) {
  df <- readr::read_csv(file, col_names = FALSE, show_col_types = FALSE)
  
  metadata_df <- data.frame(
    observation_station = character(),
    date_valid = character(),
    location_latitude = numeric(),
    location_longitude = numeric(),
    height = numeric(),
    station_type = numeric(),
    type_of_measuring_equipment = numeric(),
    type_of_antenna = numeric(),
    mean_speed_estimation = numeric(),
    wind_computation_enhancement = numeric(),
    stringsAsFactors = FALSE
  )
  
  start_rows <- which(df[, 1] == "Conventions")
  end_rows   <- which(df[, 1] == "number_of_levels")
  
  for (i in seq_along(start_rows)) {
    start_row <- start_rows[i]
    end_row   <- ifelse(i < length(end_rows), end_rows[i], nrow(df))
    metadata  <- df[start_row:end_row, ]
    
    observation_station <- metadata[metadata[, 1] == "observation_station", 3] %>% unique()
    if (length(observation_station) == 1) {
      metadata_df <- rbind(metadata_df, data.frame(
        observation_station          = as.character(observation_station),
        date_valid                   = as.character(metadata[metadata[, 1] == "date_valid", 3]),
        location_latitude            = as.numeric(metadata[metadata[, 1] == "location", 3]),
        location_longitude           = as.numeric(metadata[metadata[, 1] == "location", 4]),
        height                       = as.numeric(metadata[metadata[, 1] == "height", 3]),
        station_type                 = as.numeric(metadata[metadata[, 1] == "station_type", 4]),
        type_of_measuring_equipment  = as.numeric(metadata[metadata[, 1] == "type_of_measuring_equipment", 4]),
        type_of_antenna              = as.numeric(metadata[metadata[, 1] == "type_of_antenna", 4]),
        mean_speed_estimation        = as.numeric(metadata[metadata[, 1] == "mean_speed_estimation", 4]),
        wind_computation_enhancement = as.numeric(metadata[metadata[, 1] == "wind_computation_enhancement", 4]),
        stringsAsFactors = FALSE
      ))
    }
  }
  metadata_df
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
      # X5 = get(names(raw)[5]),
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
  library(doParallel); library(foreach); library(data.table)
  ncores  <- max(1, parallel::detectCores()-1)
  
  registerDoParallel(ncores); on.exit(stopImplicitCluster(), add = TRUE)
  message(sprintf("⏳ Processing %d files using %d cores ...", length(csv_files), ncores))
  
  nchunks <- ncores
  chunked <- split(csv_files, ceiling(seq_along(csv_files) / ceiling(length(csv_files) / nchunks)))
  
  res_list <- foreach(batch = chunked,
                      .packages = c("data.table"),
                      .export   = c("process_file"),
                      .errorhandling = "pass") %dopar% {
                        outs <- lapply(batch, function(f)
                          tryCatch(process_file(f), error = function(e) list(data=data.table(), log=data.table())))
                        list(
                          data = data.table::rbindlist(lapply(outs, `[[`, "data"), use.names = TRUE, fill = TRUE),
                          log  = data.table::rbindlist(lapply(outs, `[[`, "log" ), use.names = TRUE, fill = TRUE)
                        )
                      }
  
  message("✅ Parallel extraction complete. Combining results...")
  combined_data <- data.table::rbindlist(lapply(res_list, `[[`, "data"), use.names = TRUE, fill = TRUE)
  combined_log  <- data.table::rbindlist(lapply(res_list, `[[`, "log" ), use.names = TRUE, fill = TRUE)
  
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
combined_data <- run_all_processing(data_dir, output_file_prefix)
file_path <- file.path(data_dir, paste0(output_file_prefix, "_combined_data.csv"))
if (file.exists(file_path)) {
  combined_data <- data.table::fread(file_path)
  message("File letto correttamente: ", file_path)
} else stop(" File non trovato: ", file_path)

#  6) CLEAN + WIND METRICS 
# Turbine parameters
turbine_cut_in   <- 3.5   # m/s
turbine_cut_out  <- 25.0  # m/s

# Constants
air_density <- 1.225  # kg/m³

# Rename and clean
data.table::setnames(combined_data, old = c("station","Date_valid","X1","X6","X7"),
                     new = c("Station","Date","Height","u","v"))

combined_data[, datetime := fasttime::fastPOSIXct(Date, required.components = 5)]
combined_data[, c("u","v") := lapply(.SD, function(x) dplyr::na_if(x, -9999999)), .SDcols = c("u","v")]
combined_data <- combined_data[!is.na(u) & !is.na(v)]

# Wind functions
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
  epsilon <- sqrt(1 - (cos_diff_sum / n)^2 - (sin_diff_sum / n)^2)
  asin(epsilon) * 180 / pi * sqrt(2 * (1 - epsilon^2))
}
calculate_wind_run <- function(wind_speeds, interval_minutes) sum(wind_speeds, na.rm = TRUE) * (interval_minutes / 60)
calculate_wind_direction <- function(u, v) {
  wd <- (180/pi) * atan2(u, v) + 180
  wd <- ifelse(wd < 0, wd + 360, wd)
  wd %% 360
}

# Derived variables
combined_data[, wind_speed     := calculate_wind_speed(u, v)]
combined_data[, wind_direction := calculate_wind_direction(u, v)]
combined_data[, wind_power_density := 0.5 * air_density * (wind_speed^3)]

# Turbine operational flag
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

# Step 1: Implausible-value filter
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

#  8) METADATA LOAD 
static_metadata <- readxl::read_excel("FinalCheck_datevalid_distinct_merged_metadata.xlsx")
write.csv(static_metadata, "FinalCheck_datevalid_distinct_merged_metadata.csv", row.names = FALSE)
static_metadata <- data.table::fread("FinalCheck_datevalid_distinct_merged_metadata.csv", header = TRUE)
static_metadata[, observation_station := trimws(observation_station)]

#  9) AGGREGATION 
add_report_id <- function(df) {
  df %>% dplyr::mutate(report_id = dplyr::row_number()) %>% dplyr::select(report_id, dplyr::everything())
}

aggregate_wind_data <- function(data, interval, interval_minutes) {
  data <- data.table::copy(data)
  data[, interval := lubridate::floor_date(datetime, interval)]
  
  stations <- unique(data$Station)
  ncores   <- min(max(1, parallel::detectCores()-1), length(stations))
  doParallel::registerDoParallel(ncores); on.exit(stopImplicitCluster(), add = TRUE)
  
  stn_chunks <- split(stations, ceiling(seq_along(stations) / ceiling(length(stations) / ncores)))
  
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
        wind_run = calculate_wind_run(wind_speed, interval_minutes),
        avg_wpd  = mean(wind_power_density, na.rm = TRUE),
        turbulence_intensity = if (.N > 1 && mean(wind_speed, na.rm = TRUE) > 0)
          sd(wind_speed, na.rm = TRUE) / mean(wind_speed, na.rm = TRUE) else NA_real_,
        wind_stress = air_density * (mean(wind_speed, na.rm = TRUE)^2)
      ), by = .(Station, Height, interval)]
      ans
    })
    data.table::rbindlist(outs, use.names = TRUE, fill = TRUE)
  }
  
  result <- data.table::rbindlist(result_list, use.names = TRUE, fill = TRUE)
  result <- add_report_id(result)
  result[order(Station, interval, Height)]
}

# Build hourly
hourly_data <- aggregate_wind_data(combined_data, "hour", 60)

# Daily from hourly (require 24 hours)
daily_data <- hourly_data[, .(
  num_obs = .N,
  avg_wind_speed = mean(avg_wind_speed, na.rm = TRUE),
  max_wind_speed = max(max_wind_speed, na.rm = TRUE),
  min_wind_speed = min(min_wind_speed, na.rm = TRUE),
  sd_wind_speed  = sd(avg_wind_speed, na.rm = TRUE),
  avg_wind_direction = mean(avg_wind_direction, na.rm = TRUE),
  sd_wind_direction  = sd(avg_wind_direction, na.rm = TRUE),
  wind_run = sum(wind_run, na.rm = TRUE),
  avg_wpd   = mean(avg_wpd, na.rm = TRUE),
  turbulence_intensity = mean(turbulence_intensity, na.rm = TRUE),
  wind_stress = mean(wind_stress, na.rm = TRUE)
), by = .(Station, Height, day = as.Date(interval))][num_obs == 24]

# Availability filter
daily_summary <- daily_data[, .(total_days = .N), by = .(Station, Height)]
daily_summary[, threshold := 0.75 * total_days]
daily_data_filtered <- daily_data[daily_summary, on = .(Station, Height), nomatch = 0][ num_obs >= threshold]

# Monthly from daily filtered
monthly_data <- daily_data_filtered[, .(
  avg_wind_speed     = mean(avg_wind_speed, na.rm = TRUE),
  max_wind_speed     = max(max_wind_speed, na.rm = TRUE),
  min_wind_speed     = min(min_wind_speed, na.rm = TRUE),
  sd_wind_speed      = sd(avg_wind_speed, na.rm = TRUE),
  avg_wind_direction = mean(avg_wind_direction, na.rm = TRUE),
  sd_wind_direction  = mean(sd_wind_direction, na.rm = TRUE),
  wind_run           = sum(wind_run, na.rm = TRUE),
  avg_wpd            = mean(avg_wpd, na.rm = TRUE),
  turbulence_intensity = mean(turbulence_intensity, na.rm = TRUE),
  wind_stress        = mean(wind_stress, na.rm = TRUE)
), by = .(Station, Height, month = lubridate::floor_date(day, "month"))]

# Yearly from monthly
yearly_data <- monthly_data[, .(
  avg_wind_speed     = mean(avg_wind_speed, na.rm = TRUE),
  max_wind_speed     = max(max_wind_speed, na.rm = TRUE),
  min_wind_speed     = min(min_wind_speed, na.rm = TRUE),
  sd_wind_speed      = sd(avg_wind_speed, na.rm = TRUE),
  avg_wind_direction = mean(avg_wind_direction, na.rm = TRUE),
  sd_wind_direction  = mean(sd_wind_direction, na.rm = TRUE),
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

#  11) NORMALIZE TIME COLUMN NAMES 
for (name in c("hourly_data","daily_data","monthly_data","yearly_data")) {
  dt <- get(name)
  if      ("interval" %in% names(dt)) data.table::setnames(dt,"interval","time")
  else if ("day"      %in% names(dt)) data.table::setnames(dt,    "day","time")
  else if ("month"    %in% names(dt)) data.table::setnames(dt,  "month","time")
  else if ("year"     %in% names(dt)) data.table::setnames(dt,   "year","time")
  
  if (!inherits(dt$time, "POSIXct")) {
    dt[, time := as.POSIXct(time, tz="UTC", tryFormats=c(
      "%Y-%m-%dT%H:%M:%SZ","%Y-%m-%d %H:%M:%S","%Y-%m-%d","%Y"
    ))]
  }
  if (!"wind_run" %in% names(dt)) dt[, wind_run := NA_real_]
  assign(name, dt, inherits = TRUE)
}

#  12) NetCDF TIME UNITS MAP 
time_units_map <- list(
  hourly  = "hours since 1970-01-01 00:00:00",
  daily   = "days since 1970-01-01",
  monthly = "days since 1970-01-01",
  yearly  = "days since 1970-01-01"
)

# 13) NetCDF CREATION 
create_ncdf <- function(df, output_file, meta, time_units) {
  df <- as.data.table(df)
  station_id <- unique(df$Station)
  
  all_vars <- c(
    "avg_wind_speed","max_wind_speed","min_wind_speed",
    "avg_wind_direction","wind_run","avg_wpd","turbulence_intensity",
    "wind_stress"
  )
  
  vars_present <- intersect(all_vars, names(df))
  if (!length(vars_present)) { warning("No obs variables in ", output_file); return(NULL) }
  
  df_long <- melt(df,
                  id.vars = c("time","Station","Height"),
                  measure.vars = vars_present,
                  variable.name = "variable",
                  value.name    = "observation_value")[!is.na(observation_value)]
  
  variable_map <- list(
    avg_wind_speed     = 107L,
    max_wind_speed     = 107L,
    min_wind_speed     = 107L,
    avg_wind_direction = 106L,
    wind_run           = 143L,
    avg_wpd            = 2001L,
    turbulence_intensity     = 2006L,
    wind_stress              = 2004L
  )
  df_long[, observed_variable := unname(variable_map[variable])]
  df_long[, index := .I]
  df_long[, z_coordinate      := Height]
  df_long[, z_coordinate_type := 1L]
  df_long[, report_timestamp  := as.numeric(
    difftime(time, as.POSIXct("1970-01-01 00:00:00", tz="UTC"), units="hours")
  )]
  
  dim_index <- ncdf4::ncdim_def("index","",df_long$index,create_dimvar=TRUE)
  vars <- list(
    ncdf4::ncvar_def("observation_value","float", list(dim_index), missval=NaN, prec="float"),
    ncdf4::ncvar_def("observed_variable","short", list(dim_index)),
    ncdf4::ncvar_def("z_coordinate","float",      list(dim_index), missval=NaN, prec="float"),
    ncdf4::ncvar_def("z_coordinate_type","short", list(dim_index)),
    ncdf4::ncvar_def("report_timestamp","double", list(dim_index))
  )
  
  if (file.exists(output_file)) file.remove(output_file)
  nc <- ncdf4::nc_create(output_file, vars)
  
  ncdf4::ncvar_put(nc,"observation_value", df_long$observation_value)
  ncdf4::ncvar_put(nc,"observed_variable", df_long$observed_variable)
  ncdf4::ncvar_put(nc,"z_coordinate",      df_long$z_coordinate)
  ncdf4::ncvar_put(nc,"z_coordinate_type", df_long$z_coordinate_type)
  ncdf4::ncvar_put(nc,"report_timestamp",  df_long$report_timestamp)
  
  ncdf4::ncatt_put(nc,"report_timestamp","units",   time_units)
  ncdf4::ncatt_put(nc,"report_timestamp","calendar","gregorian")
  ncdf4::ncatt_put(nc,0,"title",      "E-PROFILE time-aggregated wind profilers' data")
  ncdf4::ncatt_put(nc,0,"institution","University of Salerno")
  ncdf4::ncatt_put(nc,0,"source",     "Wind profilers")
  ncdf4::ncatt_put(nc,0,"Source_id",  "E-PROFILE v1.0")
  ncdf4::ncatt_put(nc,0,"history",    paste("Created on", format(Sys.time())))
  ncdf4::ncatt_put(nc,0,"contact",    "Fabio Madonna - fmadonna@unisa.it")
  ncdf4::ncatt_put(nc,0,"Data_policy_licence","CC-BY-4.0")
  ncdf4::ncatt_put(nc,0,"Conventions","CF-1.8")
  ncdf4::ncatt_put(nc,0,"primary_id", station_id)
  
  if (nrow(meta)==1) {
    ncdf4::ncatt_put(nc,0,"latitude",                       meta$location_latitude)
    ncdf4::ncatt_put(nc,0,"longitude",                      meta$location_longitude)
    ncdf4::ncatt_put(nc,0,"height_of_station_above_sea_level", meta$height)
    ncdf4::ncatt_put(nc,0,"equipment_type",                 meta$type_of_measuring_equipment)
    ncdf4::ncatt_put(nc,0,"antenna_type",                   meta$type_of_antenna)
    ncdf4::ncatt_put(nc,0,"Station_automation",             meta$station_type)
    ncdf4::ncatt_put(nc,0,"wind_speed_estimation_method",   meta$mean_speed_estimation)
    ncdf4::ncatt_put(nc,0,"wind_computation_enhancement",   meta$wind_computation_enhancement)
    ncdf4::ncatt_put(nc,0,"turbine_cut_in",  as.numeric(turbine_cut_in))
    ncdf4::ncatt_put(nc,0,"turbine_cut_out", as.numeric(turbine_cut_out))
    ncdf4::ncatt_put(nc,0,"var_2004_desc", "wind_stress: tau = rho * U^2, using mean speed over the aggregation period")
  }
  
  ncdf4::nc_close(nc)
  message("Saved NetCDF: ", output_file)
}

#  14) WRITE NetCDF per STATION & LEVEL 
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
    safe     <- gsub(" ","_", stn)
    outfile  <- sprintf("%s%s_%s.nc", output_prefix, level, safe)
    create_ncdf(df_stn, outfile, meta_stn, time_units_map[[level]])
  }
}

#  END FILE 
##  PRINT TOTAL RUNTIME 
end_time <- Sys.time()
end_cpu  <- proc.time()
cat("⏱ Total wall time :", round(difftime(end_time, start_time, units = "secs"), 2), "seconds\n")
cat("💻 Total CPU time  :", round((end_cpu - start_cpu)[["elapsed"]], 2), "seconds\n")
