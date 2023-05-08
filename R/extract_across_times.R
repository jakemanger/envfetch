source('./code/data_extraction_helpers.R')

# set the gdalCache size to 30000 MB
# as opposed to the default 1632 MB
# so it can run much faster
terra::gdalCache(30000)
terraOptions(memfrac=0.9)

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
extract_across_times <- function(points, r, extract_all_times_at_start = TRUE) {
  if (extract_all_times_at_start) {
    dates <- time(r)
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

    print('Extracting data points...')

    r_within_time <-
      r[[which(dates >= min_time & dates <= max_time)]]
    nms <- names(r_within_time)
    tms <- terra::time(r_within_time)

    extracted <- terra::extract(x = r_within_time,
                                y = points)

    print('Summarising extracted data over specified times')

    new_col_names <- unique(str_split_i(nms, '_', 1))

    points[, new_col_names] <- NA

    for (i in 1:nrow(points)) {
      mn = lubridate::int_start(points$time_column[i])
      mx = lubridate::int_end(points$time_column[i])
      for (col_name in new_col_names) {
        col_names_to_summarise <-
          tms >= mn & tms <= mx & str_starts(nms, col_name)
        cols_to_summarise <-
          colnames(extracted) %in% nms[col_names_to_summarise]
        points[i, col_name] <-
          mean(as.numeric(extracted[i, cols_to_summarise]), na.rm = TRUE)
      }
    }
  } else {
    with_progress({
      p <- progressor(steps = length(unique(points$time_column)) * 2)

      extracted <- points$time_column %>%
        unique() %>%
        map(function(x) {
          extracted <-
            extr(r, points %>% filter(as.character(time_column) == as.character(x)))
          p()
          return(extracted)
        }) %>% bind_rows() %>%
        as_tibble()

      # extracted <- points %>%
      #   purrr::transpose() %>%
      #   purrr::map(function(x) {
      #     extracted <- extr(r, x)
      #     p()
      #     return(extracted)
      #   }) %>%
      #   bind_rows() %>%
      #   as_tibble()

      points <- points %>% bind_cols(extracted)
    })
  }

  return(points)
}
