#' Fetch data for each row in your dataset using the supplied functions
#'
#' Used to gather all input data necessary for modelling
#'
#' @param points A tibble with a `sf` "geometry" and a "time_column" (a `lubridate` interval),
#' @param ... Anonymous functions you would like to use on each row of the dataset.
#' @param use_cache Whether to cache your progress. Allows you to continue where you left off in case of an error or the process is interrupted.
#' @param out_dir A directory to output your result. Is ignored if out_filename = NA.
#' @param out_filename The path to output the result. Set to NA to not save the result and only return the result.
#' @param cache_dir A directory to output cached progress. Is ignored if use_cache = FALSE.
#' @param .time_rep A `time_rep` object. Used to repeat data extraction along repeating time intervals relative to the minimum time of each row in the dataset.
#'
#' @return
#' @export
#'
#' @examples
fetch <- function(
    points,
    ...,
    use_cache=TRUE,
    out_dir=file.path('./output/'),
    out_filename=paste0('output_', format(Sys.time(), "%Y_%m_%d_%H_%M_%S"), '.gpkg'),
    overwrite=TRUE,
    cache_dir=file.path(out_dir, 'cache/'),
    time_column_name='time_column',
    .time_rep=NA
) {
  message('Fetching your data... 🥏 🐕')
  progressr::handlers('progress')

  if (!dir.exists(out_dir)) dir.create(out_dir)
  if (use_cache && !dir.exists(cache_dir)) dir.create(cache_dir)

  points$time_column <- points %>% parse_time_column(time_column_name)

  col_names_used_in_func <- c('time_column', 'geometry')

  original_points <- points
  if (length(.time_rep) > 1) {
    # generate all time lag intervals we want to extract data for
    points <- points %>%
      create_time_lags(n_lag_range=c(.time_rep$n_start, .time_rep$n_end), time_lag=.time_rep$interval, relative_to_start=.time_rep$relative_to_start)
    col_names_used_in_func <- c(col_names_used_in_func, 'original_time_column', 'lag_amount')
  }

  extra_cols <- colnames(original_points)
  extra_cols <- extra_cols[!(extra_cols %in% col_names_used_in_func)]

  # remove unnecessary columns so caching can work here and to make things work faster
  points <- points %>% dplyr::select(-c(extra_cols))

  # create unique name to cache progress of extracted point data (so you can continue if you lose progress)
  hash <- rlang::hash(points)

  # capture the supplied ... arguments
  args <- c(...)

  # remove elements that aren't functions and raise a warning if any are
  # detected
  is_function <- sapply(args, function(x) {is.function(x) || purrr::is_formula(x)})
  not_funcs <- args[!is_function]
  if (length(not_funcs) > 0) {
    warning(paste(not_funcs, 'ignored as it is not a function.\n'))
  }
  funcs <- args[is_function]

  # loop through the supplied functions
  outs <- lapply(funcs, function(fun) {
    fun_string <- paste(as.character(fun), collapse='')
    # convert the formula to a function if it is a formula
    fun <- purrr::as_mapper(fun)

    # generate unique hash with dataset and function
    # to save progress
    # and either read cached output or run the function
    outpath <- file.path(cache_dir, paste0(hash, '_', rlang::hash(fun), '.rds'))
    if (!use_cache || !file.exists(outpath)) {
      out <- fun(points)
      out <- out[,!(colnames(out) %in% colnames(points))]
      out <- sf::st_drop_geometry(out)
      message(paste('🐶 Completed', fun_string))

      if (use_cache)
        saveRDS(out, outpath)
    } else {
      message(paste('🕳️🦴 Dug up cached result of', fun_string))
      out <- readRDS(outpath)
    }
    return(out)
  })

  points <- dplyr::bind_cols(c(points, outs))

  if (length(.time_rep) > 1) {
    # now take those time lagged points and set them as columns for their
    # original point
    cols_to_get_vals_from <- colnames(points)[!(colnames(points) %in% c(extra_cols, col_names_used_in_func))]
    extra_col_vals <- original_points %>% dplyr::select(c(extra_cols)) %>%
      sf::st_drop_geometry()
    points <- points %>%
      dplyr::select(-c('time_column')) %>%
      tidyr::pivot_wider(
        names_from = 'lag_amount',
        values_from = cols_to_get_vals_from,
        names_glue={"{.value}{ifelse(lag_amount != '', '_', '')}{lag_amount}"} # don't have an extra '_' at end if there was no lag for that row
      )

    if (length(extra_cols) > 0) {
      # add back any extra columns not used in the pivot_wider
      extra_col_vals <- extra_col_vals %>% dplyr::select(
        colnames(extra_col_vals)[!(colnames(extra_col_vals) %in% colnames(points))]
      )
      points <- points %>% dplyr::bind_cols(
        extra_col_vals
      )
    }
  }
  # remove columns with all NA values
  points <- points %>%
    dplyr::select(dplyr::where(function(x) any(!is.na(x))))

  if (!is.na(out_filename)) {
    out_path <- file.path(out_dir, out_filename)
    message(paste('Saving to shapefile at', out_path))
    if (overwrite && file.exists(out_path)) {
      file.remove(out_path)
    }
    if (getFileExtension(out_path) == 'csv') {
      # user probably wants x and y coordinates and this stuffs up with csv as
      # commas are used in the default output geometry column
      sf::st_write(points, out_path, append=ifelse(overwrite, FALSE, NA), layer_options = "GEOMETRY=AS_XY")
    } else {
      sf::st_write(points, out_path, append=ifelse(overwrite, FALSE, NA))
    }
    message(
      paste0(
        'Check the output directory (',
        out_dir,
        ') for your saved shapefile with your extracted points.'
      )
    )
  }

  message('Fetched!')

  return(points)
}

getFileExtension <- function(file) {
  splt <- strsplit(file, ".", fixed=T)[[1]][]
  return(splt[length(splt)])
}
