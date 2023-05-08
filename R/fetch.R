library(tidyverse)
library(sf)
library(lubridate)
library(terra)
library(luna)
library(stars)
library(furrr)
library(progressr)

source('./code/data_extraction_helpers.R')
source('./code/estimate_foo.R')
source('./code/get_daynight_times.R')
source('./code/get_clum_landuse.R')
source('./code/extract_awap_data.R')
source('./code/extract_soil_data.R')
source('./code/extract_gee.R')

handlers("progress")


#' Fetch data for each row in your dataset using the supplied functions
#'
#' Used to gather all input data necessary for modelling
#'
#' @param points A tibble with three columns: "x", "y"(
#' GPS points in lat and long EPSG 4326) and "time_column" (an interval),
#'
#' @return
#' @export
#'
#' @examples
fetch <- function(
    points,
    use_cache=TRUE,
    grouping_variables=c(
      'time_column', 'site'
    )
) {
  # create unique name to cache progress of extracted point data (so you can continue if you lose progress)
  hash <- rlang::hash(points)

  # let's first calculate/extract data that is the same for each time range

  outpath <- paste0('./output/', hash, 'extracted_day_night_stats.rds')
  if (!use_cache || !file.exists(outpath)) {
    points <- points %>% get_daynight_times(savepath=outpath)
  } else {
    points <- readRDS(outpath)
  }

  outpath <- paste0('./output/', hash, 'extracted_clum.rds')
  if (!use_cache || !file.exists(outpath)) {
    points <- points %>% get_clum_landuse(savepath=outpath)
  } else {
    points <- readRDS(outpath)
  }

  # for extracting data across different time ranges, we will create new rows with
  # each time range that we want, previously added data will be set to NA

  # generate all time lag intervals we want to extract data for
  points <- points %>%
    create_time_lags(n_lags=26, time_lag=days(14)) %>%
    distinct()

  outpath <- paste0('./output/', hash, 'extracted_modis.rds')
  if (!use_cache || !file.exists(outpath)) {
    points <- points %>% extract_gee(
      collection_name='MODIS/006/MOD13Q1',
      bands=c('NDVI', 'DetailedQA'),
      time_buffer=16,
      savepath=outpath
    )
  } else {
    points <- readRDS(outpath)
  }

  outpath <- paste0('./output/', hash, 'extracted_soil_moisture.rds')
  if (!use_cache || !file.exists(outpath)) {
    points <- points %>% extract_soil_data(extract_all_times_at_start = TRUE, savepath=outpath)
  } else {
    points <- readRDS(outpath)
  }

  # outpath <- paste0('./output/', hash, 'extracted_awap.rds')
  if (!use_cache || !file.exists(outpath)) {
    points <- points %>% extract_awap_data(extract_all_times_at_start = TRUE, savepath=outpath)
  } else {
    points <- readRDS(outpath)
  }

  outpath <- paste0('./output/', hash, 'extracted_foo.rds')
  browser()
  if (!use_cache || !file.exists(outpath)) {
    points <- points %>% estimate_foo(savepath=outpath)
  } else {
    points <- readRDS(outpath)
  }

  # now take those time lagged points and set them as columns for their
  # original point
  cols_to_get_vals_from <- colnames(points)[!(colnames(points) %in% c(grouping_variables, 'original_time_column', 'geometry', 'lag_amount'))]
  points <- points %>%
    select(-c(grouping_variables)) %>%
    pivot_wider(
      names_from = c('lag_amount'),
      values_from = cols_to_get_vals_from
  )
  # remove columns with all NA values
  points <- points %>%
    select(where(function(x) any(!is.na(x))))

  print('Check the output directory (./output) for csv files with your extracted points')

  return(points)
}
