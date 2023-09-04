#' Extract values from a raster over time
#'
#' This function extracts raster data over time ranges of each row and
#' summarises the extracted data using a custom function.
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
#' to `TRUE` to ensure the vectorised approach is used for performance. Note,
#' vectorised summarisation functions are not possible when `fun=NULL` and you
#' are extracting with polygon or line geometries (i.e. `temporal_fun` is used
#' to summarise, treating each time and space value independently).
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
#' @param is_vectorised_summarisation_function Whether the summarisation is vectorised (like rowSums or rowMeans). Is only
#' necessary to be TRUE if the row-wise vectorised summarisation function has not been automatically detected
#' (does not use rowSums or rowMeans).
#' @param parallel Whether to use parallel processing when calculating summary information for each time range.
#' @param workers The number of parallel processing workers to use for summarisation over each data point's time range.
#' @param create_parallel_plan Whether to create the `future` parallel processing plan for you. If `TRUE` (the default),
#' this will use `future::plan(future::multisession(workers = workers))` with the provided `workers` argument.
#' See https://future.futureverse.org/reference/plan.html for more parallel processing options (e.g. clusters or linux forking)
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
  is_vectorised_summarisation_function=FALSE,
  parallel=FALSE,
  workers=future::availableCores(),
  create_parallel_plan=TRUE,
  ...
) {
  if (override_terraOptions) {
    # set the gdalCache size to 30000 MB
    # as opposed to the default 1632 MB
    # so it can run much faster with big files
    terra::gdalCache(30000)
    terra::terraOptions(memfrac=0.9, progress=1)
  }
  if (parallel && create_parallel_plan)
    future::plan(future::multisession(workers = workers))

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

  gc() # added here as a lot of ram gets used with a large number of x (e.g. 10000+)

  message('Extracting data...')

  r_within_time <- r[[relevant_indices]]

  # fix layer names to make sure there are no duplicates
  # so we can easily do spatial joins later
  fixed_layer_names <- make_unique_names(names(r_within_time))
  names(r_within_time) <- fixed_layer_names

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

  # initialise variables with NA
  x[, new_col_names] <- NA

  multi_values_in_extraction_per_row <- any(table(extracted$ID) > 1)

  # make sure the ID column is last
  stopifnot(colnames(extracted)[length(colnames(extracted))] == 'ID')
  # make sure that the order of the columns in extracted have not changed
  stopifnot(all(colnames(extracted)[1:(length(colnames(extracted))-1)] == nms))

  progressr::with_progress({
    # remove the sf geometry before summarisation, as it is faster to work
    # with a dataframe
    geometry <- sf::st_geometry(x)
    x <- sf::st_drop_geometry(x)

    if (contains_rowSums_or_rowMeans(temporal_fun))
      message('Detected a vectorised row summarisation function. Using optimised summarisation approach with multiple rows as inputs.')

    if (contains_rowSums_or_rowMeans(temporal_fun) || is_vectorised_summarisation_function) {
      if (multi_values_in_extraction_per_row)
        stop('You cannot use a vectorised row summarisation function with fun=NULL when extracting with polygons or lines. Use a non-vectorised alternative for the `temporal_fun` instead, e.g. `sum`, `mean` or `function(x) {mean(x, na.rm=TRUE)}')

      x <- vectorised_summarisation(x, extracted, temporal_fun, tms, nms, time_column_name, new_col_names, parallel=parallel)
    } else {
      message('Detected a custom row summarisation function. Running on each row one by one.')
      x <- non_vectorised_summarisation(x, extracted, temporal_fun, tms, nms, time_column_name, new_col_names, multi_values_in_extraction_per_row, parallel=parallel)
    }

    sf::st_geometry(x) <- geometry
  })

  return(x)
}

vectorised_summarisation <- function(x, extracted, temporal_fun, tms, nms, time_column_name, new_col_names, parallel=TRUE) {
  p <- progressr::progressor(steps=length(unique_time_ranges))

  # directly access time ranges without pipes
  time_ranges <- x[[time_column_name]]
  time_range_starts <- lubridate::int_start(time_ranges)
  time_range_ends <- lubridate::int_end(time_ranges)
  # convert to characters for lookup speed
  time_ranges <- as.character(time_ranges)
  unique_time_ranges <- unique(time_ranges)

  summarise <- function(range) {
    i <- which(time_ranges == range)

    mn = time_range_starts[i[1]]
    mx = time_range_ends[i[1]]
    temp_df <- x[i, ]

    for (col_name in new_col_names) {
      col_names_to_summarise <- tms >= mn & tms <= mx & stringr::str_starts(nms, col_name)
      cols_to_summarise <- colnames(extracted)[col_names_to_summarise]

      to_summarise <- extracted[i, cols_to_summarise]
      if (is.vector(to_summarise)) {
        # if there is just one time slice, use that
        temp_df[col_name] <- to_summarise
      } else {
        # if there is more than one, run the temporal fun on it
        temp_df[col_name] <- temporal_fun(extracted[i, cols_to_summarise])
      }
    }

    p(amount=length(i))
    return(temp_df)
  }

  if (parallel) {
    results <- furrr::future_map(unique_time_ranges, summarise)
  } else {
    results <- purrr::map(unique_time_ranges, summarise)
  }

  x <- do.call(rbind, results)

  return(x)
}

non_vectorised_summarisation <- function(x, extracted, temporal_fun, tms, nms, time_column_name, new_col_names, multi_values_in_extraction_per_row, parallel=TRUE) {
  p <- progressr::progressor(steps=nrow(x))

  time_ranges <- x[[time_column_name]]
  time_range_starts <- lubridate::int_start(time_ranges)
  time_range_ends <- lubridate::int_end(time_ranges)

  summarise <- function(i) {
    mn = time_range_starts[i]
    mx = time_range_ends[i]
    temp_df <- x[i, ]

    for (col_name in new_col_names) {
      col_names_to_summarise <- tms >= mn & tms <= mx & stringr::str_starts(nms, col_name)
      cols_to_summarise <- colnames(extracted)[col_names_to_summarise]

      if (multi_values_in_extraction_per_row) {
        data_to_summarise <- unlist(extracted[extracted$ID == i, cols_to_summarise])
      } else {
        data_to_summarise <- unlist(extracted[i, cols_to_summarise])
      }

      temp_df[col_name] <- temporal_fun(data_to_summarise)
    }
    p()
    return(temp_df)
  }

  if (parallel) {
    results <- furrr::future_map(1:nrow(x), summarise)
  } else {
    results <- purrr::map(1:nrow(x), summarise)
  }

  x <- do.call(rbind, results)

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

# function to make names unique, as terra will produce duplicate
# time slice names if there are multiple years
make_unique_names <- function(names) {
  unique_names <- character(length(names))
  name_count <- list()

  for (i in seq_along(names)) {
    name <- names[i]

    if (is.null(name_count[[name]])) {
      name_count[[name]] <- 1
      unique_names[i] <- name
    } else {
      name_count[[name]] <- name_count[[name]] + 1
      unique_names[i] <- paste(name, name_count[[name]], sep = "_")
    }
  }

  return(unique_names)
}
