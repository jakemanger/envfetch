#' Extract values from a raster over time
#'
#' This function extracts raster data over time ranges of each row and
#' summarise the extracted data using a custom function.
#'
#' @param x An sf object containing the locations to be sampled.
#' This should contain a column of type lubridate::interval to represent the time.
#' @param r A SpatRaster object from the terra package. This is the raster data
#' source from which the data will be extracted.
#' @param temporal_fun A function used to summarise multiple data points
#' found within a time interval. Default is `rowMeans(x, na.rm=TRUE)`. The user
#' can supply vectorised summarisation functions (using rowMeans or rowSums) or
#' non-vectorised summarisation functions (e.g., `sum`, `mean`, `min`, `max`).
#' If supplying a custom vectorised `temporal_fun`, set `is_vectorised_temporal_fun`
#' to `TRUE`.
#' @param spatial_extraction_fun A function used to extract points spatially for
#' each time slice of the raster. Default is the default implementation of `extract_over_space`
#' (extracts the `mean` of points within polygons or lines, removing NAs).
#' @param time_buffer Time buffer used to adjust the time interval for data extraction.
#' The function always uses the time before and after the interval to prevent errors
#' when summarising the earliest and latest times. Default is 0 days.
#' @param debug If TRUE, pauses the function and displays a plot for each extracted
#' point. This is useful for debugging unexpected extracted values. Default is FALSE.
#' @param override_terraOptions If TRUE, overrides terra's default terraOptions
#' with those specified in the envfetch's package. Default is TRUE.
#' @param time_column_name Name of the time column in the dataset. If NULL (the default), a column of type lubridate::interval
#' is automatically selected.
#' @param is_vectorised_temporal_fun An override in case the user supplies a
#' vectorised row summarisation function (e.g., `rowMeans`) that is not
#' automatically detected.
#' @param ... Additional arguments to `spatial_extraction_fun`. For example, `na.rm=TRUE` to not include NAs in calculations or
#' `fun=sum` to use a different function to summarise spatially.
#'
#' @return A modified version of the input 'x' with additional columns
#' containing the extracted data.
#'
#' @examples
#' \dontrun{
#' \dontrun{
#' extracted <- d %>%
#'   fetch(
#'     ~extract_over_time(.x, r = '/path/to/netcdf.nc'),
#'   )
#'
#' # repeatedly extract and summarise data every fortnight for the last six months
#' # relative to the start of the time column in `d`
#' rep_extracted <- d %>%
#'   fetch(
#'       ~extract_over_time(.x, r = '/path/to/netcdf.nc'),
#'       .time_rep=time_rep(interval=lubridate::days(14), n_start=-12),
#'     )
#'   }
#' }
#' @export
extract_over_time <- function(
  x,
  r,
  temporal_fun=function(x) {rowMeans(x, na.rm=TRUE)},
  spatial_extraction_fun=function(x, r, ...) {
    extract_over_space(
      x = x,
      r = r,
      ...
    )
  },
  time_buffer=lubridate::days(0),
  debug=FALSE,
  override_terraOptions=TRUE,
  time_column_name=NULL,
  is_vectorised_summarisation_function=NULL,
  ...
) {
  if (override_terraOptions) {
    # set the gdalCache size to 30000 MB
    # as opposed to the default 1632 MB
    # so it can run much faster with big files
    terra::gdalCache(30000)
    terra::terraOptions(memfrac=0.9, progress=1)
  }

  message('Loading raster file')

  if (is.character(r)) {
    r <- terra::rast(r)
  }
  dates <- terra::time(r)

  if (is.null(time_column_name)) {
    time_column_name <- find_time_column_name(x)
  }

  time_intervals <- x %>% dplyr::pull(time_column_name)

  lubridate::int_start(time_intervals) <- lubridate::int_start(time_intervals) - time_buffer
  lubridate::int_end(time_intervals) <- lubridate::int_end(time_intervals) + time_buffer

  # get min and max times from x to check for errors
  min_time <- min(lubridate::int_start(time_intervals))
  max_time <- max(lubridate::int_end(time_intervals))

  if (min(dates) > max_time) {
    stop('All requested data are before minimum time in data source')
  }
  if (max(dates) < min_time) {
    stop('All requested data are after maximum time in data source')
  }

  # trim r and dates exteriors before finding time slices for speed
  message('Finding relevant time slices')
  r <- r[[dates <= max_time & dates >= min_time]]
  dates <- terra::time(r)
  relevant_indices <- find_relevant_time_slices(dates, time_intervals)

  message('Extracting data...')

  r_within_time <- r[[relevant_indices]]

  nms <- names(r_within_time)
  tms <- terra::time(r_within_time)

  extracted <- spatial_extraction_fun(
    x = x,
    r = r_within_time,
    ...
  )

  if (debug) {
    message('Creating debug plot')
    r_to_plot <- r_within_time[[1]]

    lims <- sf::st_bbox(x)
    r_lims <- sf::st_bbox(r_to_plot)
    lims$xmin <- min(lims$xmin, r_lims$xmin)
    lims$xmax <- max(lims$xmax, r_lims$xmax)
    lims$ymin <- min(lims$ymin, r_lims$ymin)
    lims$ymax <- max(lims$ymax, r_lims$ymax)

    terra::plot(r_to_plot, xlim=c(lims$xmin, lims$xmax), ylim=c(lims$ymin, lims$ymax))
    plot(x, axes=TRUE, add=TRUE)
    graphics::title('Sampling x and a slice of data to extract from')
    readline(prompt = "Paused as debug=TRUE, press enter to continue.")
  }

  message('Summarising extracted data over specified times')

  new_col_names <- unique(stringr::str_split_i(nms, '_', 1))

  x[, new_col_names] <- NA

  progressr::with_progress({
    time_ranges <- x %>% dplyr::pull(time_column_name)
    unique_time_ranges <- unique(time_ranges)

    p <- progressr::progressor(steps=length(unique_time_ranges))

    for (range in unique_time_ranges) {
      i <- time_ranges == range

      mn = lubridate::int_start(range)
      mx = lubridate::int_end(range)

      for (col_name in new_col_names) {
        col_names_to_summarise <-
          tms >= mn & tms <= mx & stringr::str_starts(nms, col_name)
        cols_to_summarise <-
          colnames(extracted) %in% nms[col_names_to_summarise]

        if (is_vectorised_summarisation_function || contains_rowSums_or_rowMeans(temporal_fun)) {
          message('Detected a vectorised row summarisation function. Using optimised summarisation approach with multiple rows as inputs.')
          x[i, col_name] <-
            temporal_fun(as.numeric(extracted[i, cols_to_summarise]))
        } else {
          message('Detected a custom row summarisation function. Running on each row one by one.')
          for (j in which(i)) {
            x[j, col_name] <-
              temporal_fun(as.numeric(extracted[i, cols_to_summarise]))
          }
        }
      }
      p()
    }
  })

  return(x)
}

contains_rowSums_or_rowMeans <- function(func) {
  # convert the function to a string
  func_string <- paste(deparse(body(func)), collapse='')

  # check for the presence of "rowSums" or "rowMeans"
  return(grepl("rowSums", func_string) || grepl("rowMeans", func_string))
}

pad_true <- function(vec) {
  shift_right <- c(FALSE, vec[-length(vec)])  # shift right
  shift_left <- c(vec[-1], FALSE)             # shift left
  vec <- vec | shift_right | shift_left       # or operation to combine shifts
  return(vec)
}

find_relevant_time_slices <- function(dates, time_intervals) {
  unique_time_intervals <- unique(time_intervals)
  relevant_indices <- sapply(
    dates,
    function(date) any(lubridate::`%within%`(date, unique_time_intervals))
  )
  # pad these values, so that data before and after can be used in the
  # summarisation function
  relevant_indices <- pad_true(relevant_indices)
  return(relevant_indices)
}

