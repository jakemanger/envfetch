#' Extract across times
#'
#' @param points an sf object with the columns "time_column" and "geometry" describing
#' where and when you want to extract data
#' @param r A SpatRaster (from the terra package) with the data you want to extract from
#' @param summarise_fun The function to summarise multiple data points found within a interval with. Defaults to `mean(x, na.rm=TRUE)`.
#' @param time_buffer The time buffer to use to use when finding data points to extract for use when summarising. Note the time that is located before and after that found in the dataset is always used by default to avoid errors when summarising the earliest and latest times.
#' @param debug Whether to pause and display a plot with information about data for each extracted point. Useful for debugging unexpected extracted values.
#' @param override_terraOptions Whether to override terra's terraOptions with envfetch's defaults. Defaults to True.
#'
#' @return points with extracted data column binded
#' @export
#'
#' @examples
extract_across_times <- function(points, r, summarise_fun=function(x) {mean(x, na.rm=TRUE)}, time_buffer=lubridate::days(0), debug=FALSE, override_terraOptions=TRUE) {
  if (override_terraOptions) {
    # set the gdalCache size to 30000 MB
    # as opposed to the default 1632 MB
    # so it can run much faster
    terra::gdalCache(30000)
    terra::terraOptions(memfrac=0.9, progress=1)
  }

  message('Loading raster file')
  r <- terra::rast(r)
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

  message('Finding relevant time slices')
  relevant_indices <- lubridate::`%within%`(dates, time_intervals)

  # pad these values, so that data before and after can be used in your summarisation function
  relevant_indices <- pad_true(relevant_indices)

  message('Extracting data points...')

  r_within_time <- r[[relevant_indices]]

  nms <- names(r_within_time)
  tms <- terra::time(r_within_time)

  extracted <- extract_without_overusing_ram(
    x = r_within_time,
    y = points
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
    title('Sampling points and a slice of data to extract from')
    readline(prompt = "Paused as debug=TRUE, press enter to continue.")
  }

  message('Summarising extracted data over specified times')

  new_col_names <- unique(stringr::str_split_i(nms, '_', 1))

  points[, new_col_names] <- NA

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
  }

  return(points)
}

pad_true <- function(vec) {
  shift_right <- c(FALSE, vec[-length(vec)])  # shift right
  shift_left <- c(vec[-1], FALSE)             # shift left
  vec <- vec | shift_right | shift_left       # or operation to combine shifts
  return(vec)
}

extract_without_overusing_ram <- function(x, y) {
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

  if (ram_required > ram_available) {
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
        p(message = sprintf("Completed extraction on chunk %d of %d", i, num_chunks))
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
