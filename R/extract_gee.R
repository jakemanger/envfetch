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
#' @param collection_name A character string representing the Google Earth
#' Engine image collection from which to extract data.
#' @param bands A vector of character strings representing the band names to
#' extract from the image collection.
#' @param scale A numeric value representing the scale at which to perform the
#' extraction in meters. Default is 250.
#' @param time_buffer A lubridate duration representing the amount of time to add
#' before and after each time interval when filtering the image collection.
#' Default is lubridate::days(20).
#' @param temporal_fun A function or string representing the function used to
#' summarise the data extracted for each interval. Default is 'last', which
#' returns the value closest to the start of the interval. Other built-in
#' options are 'closest' and 'next'.
#' @param debug A logical indicating whether to produce debugging plots. Default
#' is FALSE.
#' @param initialise_gee A logical indicating whether to initialise Google Earth
#' Engine within the function. Default is TRUE.
#' @param use_gcs A logical indicating whether to use Google Cloud Storage for
#' larger requests. Default is FALSE.
#' @param use_drive A logical indicating whether to use Google Drive for larger
#' requests. Default is FALSE.
#' @param max_chunk_time_day_range An string representing the maximum number of
#' time units to include in each time chunk when splitting the dataset for
#' efficient memory use on Google Earth Engine's end. Default is '6 months'.
#' @param max_feature_collection_size An integer representing the maximum number
#' of features (rows) to include in each chunk when splitting the dataset for
#' efficient memory use on Google Earth Engine's end. Default is 5000.
#' @param ee_reducer_fun A Google Earth Engine reducer function representing the
#' function used to aggregate the data extracted from each image. Default is
#' rgee::ee$Reducer$mean().
#' @param verbose Whether to print messages to the console. Defaults to TRUE.
#' @inheritParams extract_over_time
#' @param ... Additional arguments for underlying extraction function,
#' `rgee::ee_extract`.
#' @return A dataframe or sf object with the same rows as the input `x`, and new
#' columns representing the extracted data. The new column names correspond to
#' the `bands` parameter.
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
#' # extract and summarise data every fortnight for the last six months
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
  lazy=FALSE,
  debug=FALSE,
  initialise_gee=TRUE,
  use_gcs=FALSE,
  use_drive=FALSE,
  max_chunk_time_day_range='6 months',
  max_feature_collection_size=5000,
  ee_reducer_fun=rgee::ee$Reducer$mean(),
  time_column_name=NULL,
  verbose=TRUE,
  is_vectorised_summarisation_function=FALSE,
  ...
) {
  if (nrow(x) > 20000) {
    warning(
      paste(
        'GEE can slow down progress on the server side after extracting many',
        'geometries in a row. Consider splitting your extraction task into',
        'fewer geometries at a time with the `batch_size` argument to envfetch.'
      )
    )
  }

  if (!use_gcs && !use_drive && lazy) {
    stop('Lazy == TRUE requires use_gcs or use_drive to also be TRUE')
  }

  if (initialise_gee)
    rgee::ee_Initialize(gcs = use_gcs, drive = use_drive)

  # use an id column to return array back to original order at end
  x$original_order <- 1:nrow(x)

  if (is.null(time_column_name)) {
    time_column_name <- find_time_column_name(x)
  }

  # sort the data by time for efficient processing
  x <- x[order(as.Date(lubridate::int_start(x %>% dplyr::pull(time_column_name)))),]

  # convert x time column to UTC, as it is used by gee
  time_column_after_sort <- x %>% dplyr::pull(time_column_name)
  time_column_after_sort <- lubridate::interval(
    lubridate::with_tz(lubridate::int_start(time_column_after_sort), 'UTC'),
    lubridate::with_tz(lubridate::int_end(time_column_after_sort), 'UTC'),
  )
  x[,time_column_name] <- time_column_after_sort

  # chunk incorporating the start date of intervals to ensure efficient memory usage on gee's end.
  # otherwise, gee will need to extract raster data from the full time range of
  # the dataset
  x$start_time <- x %>% dplyr::pull(time_column_name) %>% lubridate::int_start() %>% as.Date()

  if (verbose)
    cli::cli_alert(cli::col_black('Splitting up data into multiple chunks for extraction'))

  pts_chunks <- split_time_chunks(x, 'start_time', max_rows=max_feature_collection_size, max_time_range=max_chunk_time_day_range)

  # figure out min and max datetime found on gee
  min_datetime <- min(lubridate::int_start(time_column_after_sort)) - time_buffer
  max_datetime <- max(lubridate::int_end(time_column_after_sort)) + time_buffer
  rgee_datetime_bounds <- check_dataset(min_datetime, max_datetime, collection_name)
  # make sure timezones are the same
  rgee_datetime_bounds <- lubridate::with_tz(rgee_datetime_bounds, tzone=lubridate::tz(min_datetime))

  cli::cli_alert(cli::col_black(paste('Split task into', length(pts_chunks), 'chunks')))

  if (verbose)
    pb <- cli::cli_progress_bar('Sending extraction tasks to Google Earth Engine', total=length(pts_chunks))

  extracteds <- list()

  for (i in seq_along(pts_chunks)) {
    chunk <- pts_chunks[[i]]
    p_feature <- rgee::sf_as_ee(sf::st_geometry(chunk))

    # get min and max dates from the x tibble
    chunk_time_column <- chunk %>% dplyr::pull(time_column_name)
    min_datetime <- min(lubridate::int_start(chunk_time_column)) - time_buffer
    max_datetime <- max(lubridate::int_end(chunk_time_column)) + time_buffer
    # if these min and max datetimes extend past the min and max in the data,
    # trip them to the bounds in the gee dataset
    min_datetime <- max(min_datetime, rgee_datetime_bounds[1])
    max_datetime <- min(max_datetime, rgee_datetime_bounds[2])
    # format the datetimes for gee
    min_datetime <- format(min_datetime, "%Y-%m-%dT%H:%M:%S")
    max_datetime <- format(max_datetime, "%Y-%m-%dT%H:%M:%S")

    # first filter by min and max date +- the time buffer
    # and filter spatially
    ic <- rgee::ee$ImageCollection(collection_name)$
      filterDate(min_datetime, max_datetime)$
      filterBounds(p_feature)

    # then select specific bands
    if (!is.null(bands)) {
      ic <- ic$select(bands)
    }

    via <- 'getInfo'

    if (use_drive)
      via <- 'drive'

    if (use_gcs)
      via <- 'gcs'

    # if supplied as rgee::ee$Reducer$mean instead of rgee::ee$Reducer$mean()
    if (is.function(ee_reducer_fun)) {
      ee_reducer_fun <- ee_reducer_fun()
    }

    if (lazy) {
      extracteds[[i]] <- rgee::ee_extract(
        x = ic,
        y = p_feature,
        scale = scale,
        fun = ee_reducer_fun,
        lazy = lazy,
        sf = FALSE,
        quiet = !debug,
        via = via,
        ...
      )
    } else {
      extracteds[[i]] <- rgee::ee_extract(
        x = ic,
        y = p_feature,
        scale = scale,
        fun = ee_reducer_fun,
        lazy = lazy,
        sf = FALSE,
        quiet = !debug,
        via = via,
        ...
      )
    }

    cli::cli_progress_update(id=pb)
  }

  for (i in seq_along(pts_chunks)) {
    extracteds[[i]] <- extracteds[[i]] %>% rgee::ee_utils_future_value()
  }

  # combine extracted data into tibble
  extracted <- dplyr::bind_rows(extracteds)

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

  # do summarisation
  if (verbose && contains_rowSums_or_rowMeans(temporal_fun))
    cli::cli_alert(cli::col_black('Detected a vectorised row summarisation function. Using optimised summarisation approach with multiple rows as inputs.'))

  if (
    contains_rowSums_or_rowMeans(temporal_fun)
    || is_vectorised_summarisation_function
    || (is.character(temporal_fun) && temporal_fun %in% c('last', 'next'))
  ) {
    x <- vectorised_summarisation(x, extracted, temporal_fun, tms, nms, time_column_name, new_col_names, time_buffer)
  } else {
    if (verbose)
      cli::cli_alert(cli::col_black('Detected a custom row summarisation function. Running on each row one by one.'))
    # x <- non_vectorised_summarisation_gee(x, extracted, temporal_fun, tms, nms, time_column_name, new_col_names)
    x <- non_vectorised_summarisation(x, extracted, NA, temporal_fun, tms, nms, time_column_name, new_col_names, time_buffer, FALSE)
  }

  # add back geometry
  sf::st_geometry(x) <- geometry

  # return x to its original order and assign back the time column
  x[,time_column_name] <- time_column_after_sort
  x <- x %>% dplyr::arrange(original_order)

  # remove unnecessary columns
  x <- x %>% dplyr::select(-c('original_order', 'start_time'))

  return(x)
}

get_date_from_gee_colname <- function(my_string) {
  if (!stringr::str_starts(my_string, 'X'))
    return(NA)

  split_string <- strsplit(my_string, "_")[[1]]
  date <- paste(split_string[1:3], collapse = "-")
  date <- stringr::str_remove(date, 'X')
  return(date)
}

split_time_chunks <- function(df, time_col='start_time', max_time_range='6 months', max_rows) {
  if (nrow(df) == 1) {
    # if there is just one datapoint, we just need one list item
    # return that now for speed and because the below code will fail otherwise
    return(list(df))
  }

  # split up data into time chunk groups
  min_time <- min(df[[time_col]])
  max_time <- max(df[[time_col]])

  breaks <- unique(
    c(
      seq(
        min_time,
        max_time,
        max_time_range
      ),
      max_time
    )
  )

  # split df into groups by datetimes
  list_of_dfs <- df %>%
    dplyr::mutate(
      group = cut(
        get(time_col),
        breaks = breaks,
        include.lowest = TRUE,
        right = FALSE
      )
    ) %>%
    dplyr::group_by(group) %>%
    dplyr::group_split()

  # split each group further if it's larger than max_rows
  list_of_dfs <- list_of_dfs %>%
    lapply(function(sub_df) {
      n <- nrow(sub_df)
      if (n <= max_rows) {
        list(sub_df)
      } else {
        group_size <- ceiling(n / max_rows)
        sub_df %>%
          dplyr::mutate(subgroup = rep(1:group_size, each = max_rows, length.out = n)) %>%
          dplyr::group_split(subgroup)
      }
    }) %>%
    unlist(recursive = FALSE)

  return(list_of_dfs)
}

check_dataset <- function(min_datetime, max_datetime, collection_name) {
  date_info <- rgee::ee_get_date_ic(rgee::ee$ImageCollection(collection_name), time_end=TRUE)
  min_date_on_rgee <- min(date_info$time_start)
  max_date_on_rgee <- max(date_info$time_end)

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
  return(c(min_date_on_rgee, max_date_on_rgee))
}
