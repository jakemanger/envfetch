#' Extract values from a raster across time
#'
#' This function extracts raster data across a specified time range. It allows
#' users to extract raster data within a given time buffer and summarise the
#' extracted data using a custom function.
#'
#' @param points An sf object containing the locations to be sampled.
#' This should contain a column 'time_column' of type lubridate::interval.
#' @param r A SpatRaster object from the terra package. This is the raster data
#' source from which the data will be extracted.
#' @param summarise_fun A function used to summarise multiple data points
#' found within a time interval. Default is `mean(x, na.rm=TRUE)`.
#' @param time_buffer Time buffer used to adjust the time interval for data extraction.
#' The function always uses the time before and after the interval to prevent errors
#' when summarising the earliest and latest times. Default is 0 days.
#' @param debug If TRUE, pauses the function and displays a plot for each extracted
#' point. This is useful for debugging unexpected extracted values. Default is FALSE.
#' @param override_terraOptions If TRUE, overrides terra's default terraOptions
#' with those specified in the envfetch's package. Default is TRUE.
#' @param chunk If TRUE, performs extraction in chunks to handle large files and
#' manage memory usage efficiently. Default is TRUE.
#'
#' @return A modified version of the input 'points' with an additional column
#' containing the extracted data.
#'
#' @examples
#' \dontrun{
#' \dontrun{
#' extracted <- d %>%
#'   fetch(
#'     ~extract_across_times(.x, r = '/path/to/netcdf.nc'),
#'   )
#'
#' # repeatedly extract and summarise data every fortnight for the last six months
#' # relative to the start of the time column in `d`
#' rep_extracted <- d %>%
#'   fetch(
#'       ~extract_across_times(.x, r = '/path/to/netcdf.nc'),
#'       .time_rep=time_rep(interval=lubridate::days(14), n_start=-12),
#'     )
#'   }
#' }
#' @export
extract_across_times <- function(
  points,
  r,
  summarise_fun=function(x) {mean(x, na.rm=TRUE)},
  time_buffer=lubridate::days(0),
  debug=FALSE,
  override_terraOptions=TRUE,
  chunk=TRUE
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
  time_intervals <- points$time_column
  lubridate::int_start(time_intervals) <- lubridate::int_start(time_intervals) - time_buffer
  lubridate::int_end(time_intervals) <- lubridate::int_end(time_intervals) + time_buffer

  # get min and max times from points to check for errors
  min_time <- min(lubridate::int_start(points$time_column))
  max_time <- max(lubridate::int_end(points$time_column))

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

  message('Extracting data points...')

  r_within_time <- r[[relevant_indices]]

  nms <- names(r_within_time)
  tms <- terra::time(r_within_time)

  extracted <- extract_without_overusing_ram(
    x = r_within_time,
    y = points,
    chunk = chunk
  )

  if (debug) {
    message('Creating debug plot')
    r_to_plot <- r_within_time[[1]]

    lims <- sf::st_bbox(points)
    r_lims <- sf::st_bbox(r_to_plot)
    lims$xmin <- min(lims$xmin, r_lims$xmin)
    lims$xmax <- max(lims$xmax, r_lims$xmax)
    lims$ymin <- min(lims$ymin, r_lims$ymin)
    lims$ymax <- max(lims$ymax, r_lims$ymax)

    terra::plot(r_to_plot, xlim=c(lims$xmin, lims$xmax), ylim=c(lims$ymin, lims$ymax))
    plot(points, axes=TRUE, add=TRUE)
    graphics::title('Sampling points and a slice of data to extract from')
    readline(prompt = "Paused as debug=TRUE, press enter to continue.")
  }

  message('Summarising extracted data over specified times')

  new_col_names <- unique(stringr::str_split_i(nms, '_', 1))

  points[, new_col_names] <- NA

  progressr::with_progress({
    p <- progressr::progressor(steps=nrow(points))
    for (i in 1:nrow(points)) {
      mn = lubridate::int_start(points$time_column[i])
      mx = lubridate::int_end(points$time_column[i])
      for (col_name in new_col_names) {
        col_names_to_summarise <-
          tms >= mn & tms <= mx & stringr::str_starts(nms, col_name)
        cols_to_summarise <-
          colnames(extracted) %in% nms[col_names_to_summarise]
        points[i, col_name] <-
          summarise_fun(as.numeric(extracted[i, cols_to_summarise]))
      }
      p()
    }

  })


  return(points)
}

pad_true <- function(vec) {
  shift_right <- c(FALSE, vec[-length(vec)])  # shift right
  shift_left <- c(vec[-1], FALSE)             # shift left
  vec <- vec | shift_right | shift_left       # or operation to combine shifts
  return(vec)
}

extract_without_overusing_ram <- function(x, y, chunk=TRUE) {
  mem_info_func <- purrr::quietly(terra::mem_info)
  mem_info <- mem_info_func(x)$result
  ram_required <- mem_info['needed']
  ram_available <- mem_info['available']

  message(
    paste(
      ram_required,
      'Kbs of RAM is required for extraction and',
      ram_available,
      'Kbs of RAM is available'
    )
  )

  if (chunk == TRUE && ram_required > ram_available) {
    # split raster into chunks based on available RAM
    times <- terra::time(x)

    num_chunks <- ceiling(ram_required / ram_available)
    chunk_size <- ceiling(length(times) / num_chunks)
    message(paste('Splitting job into', num_chunks, 'chunks'))

    progressr::with_progress({
      p <- progressr::progressor(steps = num_chunks)

      # initialize list to hold chunks
      r_chunks <- vector("list", num_chunks)

      # divide raster into chunks
      for (i in seq_len(num_chunks)) {
        start_index <- ((i - 1) * chunk_size) + 1
        end_index <- min(i * chunk_size, length(times))
        r_chunks[[i]] <- x[[start_index:end_index]]
      }


      extractions <- lapply(r_chunks, function(chunk) {
        ex <- terra::extract(x = chunk, y = y)
        # update progress bar after each extraction
        p()
        return(ex)
      })
      # perform extraction on each chunk and combine results
      extracted <- do.call(cbind, extractions)
    })
  } else {
    # perform extraction normally if raster fits in RAM
    extracted <- terra::extract(x = x, y = y)
  }
  message('Completed extraction')

  return(extracted)
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

