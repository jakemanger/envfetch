#' Extracts specified bands from an image collection using Google Earth Engine
#'
#' This function uses Google Earth Engine to extract specified bands from an
#' image collection. The function summarises this information for each row in
#' your dataset (`x`). The function handles memory constraints on Google
#' Earth Engine's end by extracting data in time chunks based on the start date
#' of each interval in the dataset. This function is best used within the
#' `fetch` function.
#'
#' @param x A `sf` collection with a geometry column and a time column.
#' @param collection_name A character string representing the Google Earth Engine
#'                        image collection from which to extract data.
#' @param bands A vector of character strings representing the band names to extract
#'              from the image collection.
#' @param scale A numeric value representing the scale at which to perform the
#'              extraction in meters. Default is 250.
#' @param time_buffer A lubridate duration representing the amount of time to add
#'                    before and after each time interval when filtering the image
#'                    collection. Default is lubridate::days(20).
#' @param temporal_fun A function or string representing the function used to summarise
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
#'                                 memory use on Google Earth Engine's end. Default is 365.
#' @param max_feature_collection_size An integer representing the maximum number of features
#'                                    (rows) to include in each chunk when splitting the
#'                                    dataset for efficient memory use on Google Earth Engine's end.
#'                                    Default is 5000.
#' @param ee_reducer_fun A Google Earth Engine reducer function representing the function
#'                       used to aggregate the data extracted from each image. Default is
#'                       rgee::ee$Reducer$mean().
#' @inheritParams extract_over_time
#' @param ... Additional arguments for underlying extraction function, `rgee::ee_extract`.
#' @return A dataframe or sf object with the same rows as the input `x`, and new columns
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
  x,
  collection_name,
  bands,
  scale=250,
  time_buffer=lubridate::days(20),
  temporal_fun='last',
  debug=FALSE,
  initialise_gee=TRUE,
  use_gcs=FALSE,
  use_drive=FALSE,
  max_chunk_time_day_range=183,
  max_feature_collection_size=500,
  ee_reducer_fun=rgee::ee$Reducer$mean(),
  time_column_name=NULL,
  parallel=FALSE,
  max_memory_per_core_mb=10000,
  workers=future::availableCores(),
  create_parallel_plan=TRUE,
  verbose=TRUE,
  ...
) {

  if (initialise_gee)
    rgee::ee_Initialize(gcs = use_gcs, drive = use_drive)

  if (parallel && create_parallel_plan) {
    options('future.globals.maxSize' = max_memory_per_core_mb * 1024 ^2)
    future::plan('future::multisession', workers = workers)
  }

  # use an id column to return array back to original order at end
  x$original_order <- 1:nrow(x)

  if (is.null(time_column_name)) {
    time_column_name <- find_time_column_name(x)
  }

  # sort the data by time for efficient processing
  x <- x[order(as.Date(lubridate::int_start(x %>% dplyr::pull(time_column_name)))),]

  # convert x time column to UTC, as it is used by gee
  time_column_after_sort <- x %>% dplyr::pull(time_column_name)
  x[,time_column_name] <- lubridate::interval(
    lubridate::with_tz(lubridate::int_start(time_column_after_sort), 'UTC'),
    lubridate::with_tz(lubridate::int_end(time_column_after_sort), 'UTC'),
  )

  # chunk incorporating the start date of intervals to ensure efficient memory usage on gee's end.
  # otherwise, gee will need to extract raster data from the full time range of
  # the dataset
  x$start_time <- x %>% dplyr::pull(time_column_name) %>% lubridate::int_start() %>% as.Date()
  pts_chunks <- split_time_chunks(x, 'start_time', max_rows=max_feature_collection_size, max_time_range=max_chunk_time_day_range)

  if (verbose)
    pb <- cli::cli_progress_bar('Extracting with Google Earth Engine', total=length(pts_chunks))

  # run extractions over each chunk
  extracteds <- lapply(pts_chunks, function(chunk) {
    p_feature <- rgee::sf_as_ee(sf::st_geometry(chunk))

    # get min and max dates from the x tibble
    chunk_time_column <- chunk %>% dplyr::pull(time_column_name)
    min_datetime <- min(lubridate::int_start(chunk_time_column)) - time_buffer
    max_datetime <- max(lubridate::int_end(chunk_time_column)) + time_buffer
    min_datetime <- format(min_datetime, "%Y-%m-%dT%H:%M:%S")
    max_datetime <- format(max_datetime, "%Y-%m-%dT%H:%M:%S")

    check_dataset(min_datetime, max_datetime, collection_name)

    ic <- rgee::ee$ImageCollection(collection_name)$
      filterBounds(p_feature)$
      filterDate(min_datetime, max_datetime)

    if (!is.null(bands)) {
      ic <- ic$select(bands)
    }

    via <- 'getInfo'

    if (use_gcs)
      via <- 'gcs'

    if (use_drive)
      via <- 'drive'

    # if supplied as rgee::ee$Reducer$mean instead of rgee::ee$Reducer$mean()
    if (is.function(ee_reducer_fun)) {
      ee_reducer_fun <- ee_reducer_fun()
    }

    extracted <- rgee::ee_extract(
      x = ic,
      y = p_feature,
      scale = scale,
      fun = ee_reducer_fun,
      lazy = FALSE,
      sf = TRUE,
      quiet = !debug,
      via = via,
      ...
    )

    # temporarily set NAs to NULL to distinguish between NAs introduced by
    # bind_rows and NAs from missing data (this case). See below.
    extracted[is.na(extracted)] <- NULL

    # drop geometry as it uses too much ram and data will be in the same order
    # as x after bind_rows
    extracted <- sf::st_drop_geometry(extracted)

    if (verbose)
      cli::cli_progress_update(id=pb)

    return(extracted)
  })

  # combine extracted data into tibble
  extracted <- dplyr::bind_rows(extracteds)
  # nas are created by bind rows without the same number of columns,
  # so we use this string trick to make sure we don't mix up no data from the
  # extract with NAs introduced by bind_rows. NAs introduced by bind_rows are
  # set to 'No sample', as we did not sample them and NAs from missing data are
  # set to NA.
  extracted[is.na(extracted)] <- 'No sample'
  extracted[is.null(extracted)] <- NA
  extracted <- NULL


  if (ncol(extracted) == 2) {
    warning('No data found in extraction from Google Earth Engine. Please check your arguments.')
  }

  if (debug) {
    missing_some_data <- apply(is.na(extracted), 1, any)
    missing_all_data <- apply(is.na(extracted), 1, all)
    extracted$missing_status <- dplyr::if_else(
      missing_all_data,
      'All missing',
      dplyr::if_else(missing_some_data, 'Some missing', 'None missing')
    )
    world <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")
    print(
      ggplot2::ggplot(data = world) +
        ggplot2::geom_sf() +
        ggplot2::geom_sf(data = x, ggplot2::aes(color = missing_status), size = 2) +
        ggplot2::scale_color_manual(values = c("blue", "orange", "red")) +
        ggplot2::labs(color = "Missing Data") +
        ggplot2::theme_minimal()
    )
  }

  if (verbose)
    cli::cli_alert(cli::col_black('Summarising extracted data over specified times'))

  # prepare data needed for summarisation
  extracted <- extracted[,stringr::str_starts(colnames(extracted), 'X')]
  clnames <- colnames(extracted)
  tms <- as.Date(unlist(lapply(clnames, get_date_from_gee_colname)))
  nms <- stringr::str_split_i(clnames, '_', 4)
  new_col_names <- unique(nms)

  geometry <- sf::st_geometry(x)
  x[, new_col_names] <- NA
  x <- sf::st_drop_geometry(x)

  # summarise
  x <- non_vectorised_summarisation_gee(x, extracted, temporal_fun, tms, nms, time_column_name, new_col_names, parallel=parallel)

  # add back geometry
  sf::st_geometry(x) <- geometry

  # return x to its original order and assign back the time column
  x[,time_column_name] <- time_column_after_sort
  x <- x %>% dplyr::arrange(original_order)

  # remove unnecessary columns
  x <- x %>% dplyr::select(-c('original_order', 'start_time'))

  return(x)
}

non_vectorised_summarisation_gee <- function(x, extracted, temporal_fun, tms, nms, time_column_name, new_col_names, parallel=TRUE) {
  pb <- cli::cli_progress_bar('Summarising extracted data with temporal_fun', total=nrow(x))

  x_time_column <- x[[time_column_name]]
  time_range_starts <- lubridate::int_start(x_time_column)

  summarise <- function(i) {
    mn = time_range_starts[i]
    temp_df <- x[i, ]

    for (col_name in new_col_names) {
      row_times <- tms[nms == col_name]
      row_values <- extracted[i, nms == col_name]
      indx <- is.na(row_values) | row_values != 'No sample'
      row_times <- row_times[indx]
      row_values <- row_values[indx]
      row_values <- as.numeric(row_values)

      if (is.character(temporal_fun) && temporal_fun == 'last') {
        last_index <- find_closest_datetime(row_times, mn, find_closest_previous=TRUE)
        value <- row_values[last_index]
        if (length(value) == 0) {
          warning('last value not found in extracted data. Increase your time_buffer to get a correct result. Returning NA')
          value <- NA
        }
        temp_df[col_name] <- value
      } else {
        index <- lubridate::`%within%`(row_times, x_time_column[i])
        temp_df[col_name] <- temporal_fun(row_values[index])
      }
    }
    cli::cli_progress_update(id=pb)
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
    warning(
      paste0(
        'Minimum date of ', min_datetime, ' in input is less than the',
        ' minimum date of the image collection, ', collection_name,
        '(', min_date_on_rgee, ')'
      )
    )
  }
  if (max_date_on_rgee < max_datetime) {
    warning(
      paste0(
        'Maximum date of ', max_datetime, ' in input is more than the',
        ' maximum date of the image collection, ', collection_name,
        '(', max_date_on_rgee, ')'
      )
    )
  }
  if (max_date_on_rgee < min_datetime) {
    warning(
      paste0(
        'Minimum date of ', min_datetime, ' in input is greater than the',
        ' maximum date of the image collection, ', collection_name,
        '(', max_date_on_rgee, ')'
      )
    )
  }
  if (min_date_on_rgee > max_datetime) {
    warning(
      paste0(
        'Maximum date of ', max_datetime, ' in input is less than the',
        ' minimum date of the image collection, ', collection_name,
        '(', min_date_on_rgee, ')'
      )
    )
  }
}
