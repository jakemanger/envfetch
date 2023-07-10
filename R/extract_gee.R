#' Extract from google earth engine
#'
#' Use google earth engine to extract your chosen image collection bands
#' and then summarise this information for each row in your dataset (`points`)
#'
#' @param points
#' @param collection_name
#' @param bands
#' @param scale
#' @param time_buffer
#' @param time_summarise_fun
#' @param debug
#' @param use_gcs
#' @param use_drive
#' @param max_feature_collection_size
#' @param ee_reducer_fun
#'
#' @return
#' @export
#'
#' @examples
extract_gee <- function(
  points,
  collection_name,
  bands,
  scale=250,
  time_buffer=lubridate::days(20),
  time_summarise_fun='last',
  debug=FALSE,
  use_gcs=FALSE,
  use_drive=FALSE,
  max_chunk_time_day_range=128,
  max_feature_collection_size=10000,
  ee_reducer_fun=rgee::ee$Reducer$mean(),
  cache_progress=TRUE,
  cache_dir='./'
) {
  rgee::ee_Initialize(gcs = use_gcs, drive = use_drive)

  pts <- points
  pts$original_order <- 1:nrow(pts)  # use a id column to return array back to original order

  # chunk incorporating the start date of intervals to ensure efficient memory usage on gee's end
  # otherwise, gee will need to extract raster data from the full time range of
  # the dataset
  pts$start_time <- as.Date(lubridate::int_start(pts$time_column))
  pts_chunks <- split_time_chunks(pts, 'start_time', max_rows=max_feature_collection_size, max_time_range=max_chunk_time_day_range)

  progressr::with_progress({
    p <- progressr::progressor(steps = length(pts_chunks)*3)

    temps <- lapply(pts_chunks, function(chunk) {
      p('Loading sf object on gee...')
      p_feature <- rgee::sf_as_ee(sf::st_geometry(chunk))

      # get min and max dates from the points tibble
      min_date <- as.character(as.Date(min(lubridate::int_start(chunk$time_column)) - time_buffer))
      max_date <- as.character(as.Date(max(lubridate::int_end(chunk$time_column)) + time_buffer))

      p(paste('Loading image collection object:', collection_name, 'with bands', paste(bands, collapse = ', ')))

      ic <- rgee::ee$ImageCollection(collection_name)$
        filterBounds(p_feature)$
        filterDate(min_date, max_date)$
        select(bands)

      p('extracting...')
      temp <- rgee::ee_extract(
        x = ic,
        y = p_feature,
        scale = scale,
        fun = ee_reducer_fun,
        lazy = FALSE,
        sf = TRUE
      )

      # use this to make sure the correct columns
      # are sampled below
      temp[is.na(temp)] <- 'No data'
      return(temp)
    })

    temp <- dplyr::bind_rows(temps)
    # nas are created by bind rows without the same number of columns,
    # so we use this string trick to make sure we don't mix up no data with
    # NAs introduced by bind_rows
    temp[is.na(temp)] <- 'No sample'
    temp_no_geom <- temp %>% sf::st_drop_geometry()
    temp_no_geom[temp_no_geom=='No data'] <- NA
    sf::st_geometry(temp_no_geom) <- sf::st_geometry(temp)
    temp <- temp_no_geom
  })

  # return to original order
  changed_order <- pts_chunks %>% bind_rows() %>% sf::st_drop_geometry() %>% select('original_order')
  pts <- pts[changed_order$original_order,]
  points <- NULL # to avoid accidentally using this in future

  if (ncol(temp) == 2) {
    warning('No data found in extraction from Google Earth Engine. Please check your arguments.')
  }

  if (debug) {
    missing_some_data <- apply(is.na(sf::st_drop_geometry(temp)), 1, any)
    missing_all_data <- apply(is.na(sf::st_drop_geometry(temp)), 1, all)
    temp$missing_status <- dplyr::if_else(
      missing_all_data,
      'All missing',
      dplyr::if_else(missing_some_data, 'Some missing', 'None missing')
    )
    world <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")
    ggplot2::ggplot(data = world) +
      ggplot2::geom_sf() +
      ggplot2::geom_sf(data = temp, ggplot2::aes(color = missing_status), size = 2) +
      ggplot2::scale_color_manual(values = c("blue", "orange", "red")) +
      ggplot2::labs(color = "Missing Data") +
      ggplot2::theme_minimal()
  }

  message('Summarising extracted data over specified times')

  geometry <- temp %>% sf::st_geometry() %>% sf::st_coordinates()
  temp_for_indxing <- temp[,stringr::str_starts(colnames(temp), 'X')] %>% sf::st_drop_geometry()
  clnames <- colnames(temp_for_indxing)
  tms <- as.Date(unlist(lapply(clnames, get_date_from_gee_colname)))
  nms <- stringr::str_split_i(clnames, '_', 4)

  new_col_names <- unique(nms)

  pts[, new_col_names] <- NA
  # pts_geom <- sf::st_geometry(pts) %>% sf::st_coordinates()

  progressr::with_progress({
    p <- progressr::progressor(steps = nrow(pts))

    for (i in 1:nrow(pts)) {
      mn = lubridate::int_start(pts$time_column[i])
      mx = lubridate::int_end(pts$time_column[i])

      for (col_name in new_col_names) {
        row_times <- tms[nms == col_name]
        row_values <- temp_for_indxing[i, nms == col_name]
        row_times <- row_times[row_values != 'No sample']
        row_values <- row_values[row_values != 'No sample']

        if (time_summarise_fun == 'last') {
          last_index <- find_closest_date(row_times, mn, find_closest_previous=TRUE)
          value <- row_values[last_index]
          if (length(value) == 0) {
            browser()
            stop('last value not found in extracted data. Increase your time_buffer to get a correct result')
          }
          pts[i, col_name] <- value
        } else {
          pts[i, col_name] <- summarisation_fun(row_values)
        }
      }
      p()
    }

  })

  return(pts)
}

find_closest_date <- function(dates, x, find_closest_previous=TRUE) {
  dates <- as.Date(dates)
  x <- as.Date(x)
  if (find_closest_previous) {
    # add filter to only include dates earlier than x
    return(which(abs(dates[dates < x]-x) == min(abs(dates[dates < x] - x))))
  } else {
    return(which(abs(dates-x) == min(abs(dates - x))))
  }
}


get_date_from_gee_colname <- function(my_string) {
  if (!stringr::str_starts(my_string, 'X'))
    return(NA)

  split_string <- strsplit(my_string, "_")[[1]]
  date <- paste(split_string[1:3], collapse = "-")
  date <- stringr::str_remove(date, 'X')
  return(date)
}

split_time_chunks <- function(df, time_col='start_time', max_time_range, max_rows) {
  # sort the data by time
  df <- df[order(df[[time_col]]),]

  # initialize an empty list to hold the chunks
  list_of_dfs <- list()

  while (nrow(df) > 0) {
    # get the index of the last row within the max time range
    last_row_in_range <- sum(df[[time_col]] <= (df[[time_col]][1] + days(max_time_range)))

    # if that is greater than the max number of rows, get only the first max_rows
    if (last_row_in_range > max_rows) {
      last_row_in_range <- max_rows
    }

    # make a chunk and remove it from the df
    chunk <- df[1:last_row_in_range,]
    df <- df[-(1:last_row_in_range),]

    # append the chunk to the list
    list_of_dfs <- c(list_of_dfs, list(chunk))
  }

  return(list_of_dfs)
}
