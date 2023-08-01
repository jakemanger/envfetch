#' Extracts specified bands from an image collection using Google Earth Engine
#'
#' This function uses Google Earth Engine to extract specified bands from an
#' image collection. The function summarises this information for each row in
#' your dataset (`points`). The function handles memory constraints on Google
#' Earth Engine's end by extracting data in time chunks based on the start date
#' of each interval in the dataset. This function is best used within the
#' `fetch` function.
#'
#' @param points An sf object containing the locations to be sampled.
#'               This should contain a time column of type lubridate::interval.
#' @param collection_name A character string representing the Google Earth Engine
#'                        image collection from which to extract data.
#' @param bands A vector of character strings representing the band names to extract
#'              from the image collection.
#' @param scale A numeric value representing the scale at which to perform the
#'              extraction in meters. Default is 250.
#' @param time_buffer A lubridate duration representing the amount of time to add
#'                    before and after each time interval when filtering the image
#'                    collection. Default is lubridate::days(20).
#' @param summarise_fun A function or string representing the function used to summarise
#'                      the data extracted for each interval. Default is 'last', which returns
#'                      the value closest to the start of the interval.
#' @param debug A logical indicating whether to produce debugging plots. Default is FALSE.
#' @param initialise_gee A logical indicating whether to initialise Google Earth Engine
#'                       within the function. Default is TRUE.
#' @param use_gcs A logical indicating whether to use Google Cloud Storage for
#'                larger requests. Default is FALSE.
#' @param use_drive A logical indicating whether to use Google Drive for larger
#'                  requests. Default is FALSE.
#' @param max_chunk_time_day_range An integer representing the maximum number of days to include
#'                                 in each time chunk when splitting the dataset for efficient
#'                                 memory use on Google Earth Engine's end. Default is 128.
#' @param max_feature_collection_size An integer representing the maximum number of features
#'                                    (rows) to include in each chunk when splitting the
#'                                    dataset for efficient memory use on Google Earth Engine's end.
#'                                    Default is 10000.
#' @param ee_reducer_fun A Google Earth Engine reducer function representing the function
#'                       used to aggregate the data extracted from each image. Default is
#'                       rgee::ee$Reducer$mean().
#' @param cache_progress A logical indicating whether to cache the progress of the extraction
#'                       for resuming in case of interruptions. Default is TRUE.
#' @param cache_dir A string representing the directory in which to save the cache files.
#'                  Default is './'.
#' @param time_column_name Name of the time column in the dataset. If NULL (the default), a column of type lubridate::interval
#'                         is automatically selected.
#' @return A dataframe or sf object with the same rows as the input `points`, and new columns
#'         representing the extracted data. The new column names correspond to the `bands` parameter.
#' @export
#'
#' @examples
#' \dontrun{
#' #' extracted <- d %>%
#'   fetch(
#'     ~extract_gee(
#'        .x,
#'        collection_name='MODIS/061/MOD13Q1',
#'        bands=c('NDVI', 'DetailedQA'),
#'        time_buffer=16,
#'      )
#'   )
#'
#' # repeatedly extract and summarise data every fortnight for the last six months
#' # relative to the start of the time column in `d`
#' rep_extracted <- d %>%
#'   fetch(
#'     ~extract_gee(
#'        .x,
#'        collection_name='MODIS/061/MOD13Q1',
#'        bands=c('NDVI', 'DetailedQA'),
#'        time_buffer=16,
#'      ),
#'     .time_rep=time_rep(interval=lubridate::days(14), n_start=-12),
#'   )
#'}
extract_gee <- function(
  points,
  collection_name,
  bands,
  scale=250,
  time_buffer=lubridate::days(20),
  summarise_fun='last',
  debug=FALSE,
  initialise_gee=TRUE,
  use_gcs=FALSE,
  use_drive=FALSE,
  max_chunk_time_day_range=128,
  max_feature_collection_size=10000,
  ee_reducer_fun=rgee::ee$Reducer$mean(),
  cache_progress=TRUE,
  cache_dir='./',
  time_column_name=NULL
) {
  if (initialise_gee)
    rgee::ee_Initialize(gcs = use_gcs, drive = use_drive)

  points$original_order <- 1:nrow(points)  # use a id column to return array back to original order

  if (is.null(time_column_name)) {
    time_column_name <- find_time_column_name(points)
  }

  # sort the data by time for efficient processing
  points <- points[order(as.Date(lubridate::int_start(points %>% dplyr::pull(time_column_name)))),]

  # convert points time column to UTC, as it is used by gee
  time_column_after_sort <- points %>% dplyr::pull(time_column_name)
  points[,time_column_name] <- lubridate::interval(
    lubridate::with_tz(lubridate::int_start(time_column_after_sort), 'UTC'),
    lubridate::with_tz(lubridate::int_end(time_column_after_sort), 'UTC'),
  )

  # chunk incorporating the start date of intervals to ensure efficient memory usage on gee's end
  # otherwise, gee will need to extract raster data from the full time range of
  # the dataset
  points$start_time <- as.Date(points %>% dplyr::pull(time_column_name))
  pts_chunks <- split_time_chunks(points, 'start_time', max_rows=max_feature_collection_size, max_time_range=max_chunk_time_day_range)

  progressr::with_progress({
    p <- progressr::progressor(steps = length(pts_chunks)*3)

    temps <- lapply(pts_chunks, function(chunk) {
      p('Loading sf object on gee...')
      p_feature <- rgee::sf_as_ee(sf::st_geometry(chunk))

      # get min and max dates from the points tibble
      chunk_time_column <- chunk %>% dplyr::pull(time_column_name)
      min_datetime <- min(lubridate::int_start(chunk_time_column)) - time_buffer
      max_datetime <- max(lubridate::int_end(chunk_time_column)) + time_buffer
      min_datetime <- format(min_datetime, "%Y-%m-%dT%H:%M:%S")
      max_datetime <- format(max_datetime, "%Y-%m-%dT%H:%M:%S")

      p(paste('Loading image collection object:', collection_name, 'with bands', paste(bands, collapse = ', ')))

      check_dataset(min_datetime, max_datetime, collection_name)

      ic <- rgee::ee$ImageCollection(collection_name)$
        filterBounds(p_feature)$
        filterDate(min_datetime, max_datetime)$
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
    temp_no_geom <- NULL
  })

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

  temp_for_indxing <- temp[,stringr::str_starts(colnames(temp), 'X')] %>% sf::st_drop_geometry()
  clnames <- colnames(temp_for_indxing)
  tms <- as.Date(unlist(lapply(clnames, get_date_from_gee_colname)))
  nms <- stringr::str_split_i(clnames, '_', 4)

  new_col_names <- unique(nms)

  points[, new_col_names] <- NA

  progressr::with_progress({
    p <- progressr::progressor(steps = nrow(points))

    points_time_column <- points %>% dplyr::pull(time_column_name)
    for (i in 1:nrow(points)) {
      mn = lubridate::int_start(points_time_column[i])
      mx = lubridate::int_end(points_time_column[i])

      for (col_name in new_col_names) {
        row_times <- tms[nms == col_name]
        row_values <- temp_for_indxing[i, nms == col_name]
        row_times <- row_times[row_values != 'No sample']
        row_values <- row_values[row_values != 'No sample']
        row_values <- as.numeric(row_values)

        if (is.character(summarise_fun) && summarise_fun == 'last') {
          last_index <- find_closest_datetime(row_times, mn, find_closest_previous=TRUE)
          value <- row_values[last_index]
          if (length(value) == 0) {
            stop('last value not found in extracted data. Increase your time_buffer to get a correct result')
          }
          points[i, col_name] <- value
        } else {
          index <- lubridate::`%within%`(row_times, points_time_column[i])
          points[i, col_name] <- summarise_fun(row_values[index])
        }
      }
      p()
    }
  })

  # return points to its original order and assign back the time column
  points[,time_column_name] <- time_column_after_sort
  points <- points %>% dplyr::arrange(original_order)

  # remove unnecessary columns
  points <- points %>% dplyr::select(-c('original_order', 'start_time'))

  return(points)
}

find_closest_datetime <- function(dates, x, find_closest_previous=TRUE) {
  dates <- lubridate::as_datetime(dates)
  x <- lubridate::as_datetime(x)
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

  # initialize an empty list to hold the chunks
  list_of_dfs <- list()

  while (nrow(df) > 0) {
    # get the index of the last row within the max time range
    last_row_in_range <- sum(df[[time_col]] <= (df[[time_col]][1] + lubridate::days(max_time_range)))

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

check_dataset <- function(min_datetime, max_datetime, collection_name) {
  date_info <- rgee::ee_get_date_ic(rgee::ee$ImageCollection(collection_name), time_end=TRUE)
  min_date_on_rgee <- min(date_info$time_start)
  max_date_on_rgee <- max(date_info$time_end)
  if (min_date_on_rgee > min_datetime) {
    stop(
      paste0(
        'Minimum date of ', min_datetime, ' in input is less than the',
        ' minimum date of the image collection, ', collection_name,
        '(', min_date_on_rgee, ')'
      )
    )
  }
  if (max_date_on_rgee < max_datetime) {
    stop(
      paste0(
        'Maximum date of ', max_datetime, ' in input is more than the',
        ' maximum date of the image collection, ', collection_name,
        '(', max_date_on_rgee, ')'
      )
    )
  }
  if (max_date_on_rgee < min_datetime) {
    stop(
      paste0(
        'Minimum date of ', min_datetime, ' in input is greater than the',
        ' maximum date of the image collection, ', collection_name,
        '(', max_date_on_rgee, ')'
      )
    )
  }
  if (min_date_on_rgee > max_datetime) {
    stop(
      paste0(
        'Maximum date of ', max_datetime, ' in input is less than the',
        ' minimum date of the image collection, ', collection_name,
        '(', min_date_on_rgee, ')'
      )
    )
  }
}
