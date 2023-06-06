#' Extract across times
#'
#' @param points an sf object with the columns "time_column" and "geometry" describing
#' where and when you want to extract data
#' @param r A SpatRaster (from the terra package) with the data you want to extract from
#' @param extract_all_times_at_start boolean (for speed optimisation). for best
#' speed, set to true if there are more different times than there are points.
#' set to false if there are more points than there are times.
#'
#' @return points with extracted data column binded
#' @export
#'
#' @examples
extract_across_times <- function(points, r, extract_all_times_at_start = TRUE, method='simple', debug=FALSE, override_terraoptions=TRUE) {
  if (override_terraOptions) {
    # set the gdalCache size to 30000 MB
    # as opposed to the default 1632 MB
    # so it can run much faster
    terra::gdalCache(30000)
    terra::terraOptions(memfrac=0.9, progress=1)
  }

  message('Loading raster file')
  r <- terra::rast(r)
  if (extract_all_times_at_start) {
    dates <- terra::time(r)
    # get min and max times from points
    min_time <- min(lubridate::int_start(points$time_column))
    max_time <- max(lubridate::int_end(points$time_column))

    data_time_range <- c(min(dates), max(dates))

    if (min(dates) > max_time) {
      stop('All requested data are before minimum time in data source')
    }
    if (max(dates) < min_time) {
      stop('All requested data are after maximum time in data source')
    }

    message('Extracting data points...')

    r_within_time <-
      r[[which(dates >= min_time & dates <= max_time)]]

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
          mean(as.numeric(extracted[i, cols_to_summarise]), na.rm = TRUE)
      }
    }
  } else {
    progressr::with_progress({
      p <- progressr::progressor(steps = length(unique(points$time_column)) * 2)

      extracted <- points$time_column %>%
        unique() %>%
        map(function(x) {
          extracted <-
            extr(r, points %>% dplyr::filter(as.character(time_column) == as.character(x)))
          p()
          return(extracted)
        }) %>% bind_rows::bind_rows() %>%
        tibble::as_tibble()

      # extracted <- points %>%
      #   purrr::transpose() %>%
      #   purrr::map(function(x) {
      #     extracted <- extr(r, x)
      #     p()
      #     return(extracted)
      #   }) %>%
      #   bind_rows() %>%
      #   as_tibble()

      points <- points %>% dplyr::bind_cols(extracted)
    })
  }

  return(points)
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
        start_time <- times[((i - 1) * chunk_size) + 1]
        end_time <- times[min(i * chunk_size, length(times))]
        r_chunks[[i]] <- x[[which(times >= start_time & times <= end_time)]]
      }

      # perform extraction on each chunk and combine results
      extracted <- do.call(rbind, lapply(r_chunks, function(chunk) {
        ex <- terra::extract(x = chunk, y = y)
        # update progress bar after each extraction
        p(message = sprintf("Completed extraction on chunk %d of %d", i, num_chunks))
        return(ex)
      }))
      }
    )
  } else {
    # perform extraction normally if raster fits in RAM
    extracted <- terra::extract(x = x, y = y)
  }
  message('Completed extraction')

  return(extracted)
}
