#' Fetch data from each row using anonymous functions
#'
#' This function passes your data through your supplied extraction functions,
#' caches progress, so that if your function crashes somewhere, you can continue where you left off,
#' shows progress and estimated time to completion and
#' allows you to repeat sampling across different times.
#'
#' @param x A tibble with a `sf` "geometry" and a column with time (a `lubridate` interval or date), specified by the `time_column_name` parameter.
#' @param ... Anonymous functions you would like to use on each row of the dataset.
#' @param use_cache Whether to cache your progress. Allows you to continue where you left off in case of an error or the process is interrupted.
#' Also avoids recomputing extractions between R sessions.
#' @param out_dir A directory to output your result. Is ignored if out_filename = NA.
#' @param out_filename The path to output the result. Set to NA (the default) to not save the result and only return the result.
#' @param overwrite Overwrite output file if exists.
#' @param cache_dir A directory to output cached progress. Is ignored if use_cache = FALSE.
#' @param time_column_name Name of the time column in the dataset. If NULL (the default), a column of type lubridate::interval
#' is automatically selected.
#' @param .time_rep A `time_rep` object. Used to repeat data extraction along repeating time intervals before and after the original datetime.
#' This can be relative to the start or the end of the input time interval (specified by the `relative_to_start` argument of `time_rep`). Defaults to the start.
#' @param verbose Whether to print progress and what stage of the extraction you are. Default is TRUE.
#'
#' @return tibble An augmented tibble with additional data fetched using supplied functions.
#'
#' @examples
#' \dontrun{
#' extracted <- d %>%
#'   fetch(
#'     ~extract_across_times(.x, r = '/path/to/netcdf.nc'),
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
#'     ~extract_across_times(.x, r = '/path/to/netcdf.nc'),
#'     ~extract_gee(
#'        .x,
#'        collection_name='MODIS/061/MOD13Q1',
#'        bands=c('NDVI', 'DetailedQA'),
#'        time_buffer=16,
#'      ),
#'     .time_rep=time_rep(interval=lubridate::days(14), n_start=-12),
#'   )
#'}
#' @export
fetch <- function(
    x,
    ...,
    use_cache=TRUE,
    out_dir=file.path('./output/'),
    out_filename=NA,
    overwrite=TRUE,
    cache_dir=file.path(out_dir, 'cache/'),
    time_column_name=NULL,
    .time_rep=NA
) {

  verbose <- list(...)$verbose
  if (length(verbose) == 0)
    verbose = TRUE

  if (verbose)
    cli::cli_h1(cli::col_black('🥏 🐕 Fetching your data'))

  if (!dir.exists(out_dir)) dir.create(out_dir)
  if (use_cache && !dir.exists(cache_dir)) dir.create(cache_dir)

  simple_extraction <- FALSE

  if (is.null(time_column_name)) {
    time_column_name <- find_time_column_name(x)
  } else if (time_column_name == 'env_extract__SKIP') {
    if (verbose)
      cli::cli_alert_info(cli::col_black('Doing simple extraction over all time slices'))
    simple_extraction <- TRUE
    stop('Simple extraction not yet implemented.')
  }

  if (verbose)
    cli::cli_alert(cli::col_black('Parsing time column'))
  x[,time_column_name] <- x %>% parse_time_column(time_column_name)

  geometry_column_name = attr(x, "sf_column")
  col_names_used_in_func <- c(time_column_name, geometry_column_name, 'row_num')

  original_x <- x

  # add a row number column for unique pivoting later on in case of duplicates
  # in input, `x`
  x <- x %>% dplyr::mutate(
    row_num = dplyr::row_number()
  )

  if (length(.time_rep) > 1) {
    if (verbose)
      cli::cli_alert_info(cli::col_black('Creating time lagged points'))
    # generate all time lag intervals we want to extract data for
    x <- x %>%
      create_time_lags(n_lag_range=c(.time_rep$n_start, .time_rep$n_end), time_lag=.time_rep$interval, relative_to_start=.time_rep$relative_to_start, time_column_name=time_column_name)
    col_names_used_in_func <- c(col_names_used_in_func, 'envfetch__original_time_column', 'lag_amount')
  }

  extra_cols <- colnames(original_x)
  extra_cols <- extra_cols[!(extra_cols %in% col_names_used_in_func)]

  # remove unnecessary columns so caching can work here and to make things work faster
  x <- x %>% dplyr::select(-c(extra_cols))

  # create unique name to cache progress of extracted point data (so you can continue if you lose progress)
  hash <- rlang::hash(x)


  # capture the supplied ... arguments as a list to preserve names
  args <- c(...)

  # remove elements that aren't functions and raise a warning if any are
  is_function <- sapply(args, function(x) {is.function(x) || purrr::is_formula(x)})
  if (!is.null(names(args))) {
    unnamed <- names(args) == '' | startsWith(names(args), '...') # in case an argument is a function
    is_function <- is_function & unnamed
  }

  not_funcs <- args[!is_function]
  funcs <- args[is_function]

  x <- x %>% dplyr::group_by(across(c(time_column_name, geometry_column_name))) %>%
    dplyr::mutate(envfetch__duplicate_ID = dplyr::cur_group_id()) %>%
    dplyr::ungroup()

  unique_x <- x[!duplicated(x$envfetch__duplicate_ID),]

  # loop through the supplied functions
  outs <- lapply(funcs, function(fun) {
    if (purrr::is_formula(fun)) {
      fun_string <- paste(as.character(fun), collapse='')
      # convert the formula to a function
      fun <- purrr::as_mapper(fun)
    } else {
      fun_string <- paste(deparse(fun), collapse='')
      fun_string <- gsub("\\s+", " ", fun_string)
    }

    # generate unique hash with dataset and function
    # to save progress
    # and either read cached output or run the function
    function_env <- environment(fun)
    function_args <- sapply(ls(function_env), function(x) {
      out <- get(x, envir = function_env)
      is_function <- is.function(out) || purrr::is_formula(out)
      if (is_function) {
        out <- deparse(out) # bytecode strings can mess up unique cache hashes the second time you run it
      }
      return(out)
    })

    outpath <- file.path(cache_dir, paste0(hash, '_', rlang::hash(capture.output(c(fun_string, function_args))), '.rds'))

    if (!use_cache || !file.exists(outpath)) {
      if (verbose)
        cli::cli_alert_info(cli::col_blue(paste('Running', '{fun_string}')))

      # Use do.call to pass not_funcs as additional arguments to fun
      out <- do.call(fun, c(list(.x = unique_x), not_funcs))

      out <- out[,!(colnames(out) %in% colnames(unique_x))]
      out <- sf::st_drop_geometry(out)
      if (verbose)
        cli::cli_alert_success(cli::col_green(paste('🐶 Completed', fun_string)))

      if (use_cache)
        saveRDS(out, outpath)
    } else {
      if (verbose)
        cli::cli_alert_success(cli::col_green(paste('🕳️🦴 Dug up cached result of', fun_string)))
      out <- readRDS(outpath)
    }
    return(out)
  })

  unique_x <- dplyr::bind_cols(c(unique_x, outs))

  # add back duplicate rows using the data from the duplicate that was
  # used in the extraction
  cols_to_remove <- colnames(x)[!(colnames(x) %in% c('envfetch__duplicate_ID'))]
  x <- dplyr::left_join(
    x,
    unique_x %>% dplyr::select(-cols_to_remove),
    by = "envfetch__duplicate_ID"
  ) %>% dplyr::select(-c('envfetch__duplicate_ID'))

  if (length(.time_rep) > 1) {
    # now take those time lagged x and set them as columns for their
    # original point
    cols_to_get_vals_from <- colnames(x)[!(colnames(x) %in% c(extra_cols, col_names_used_in_func))]
    extra_col_vals <- original_x %>% dplyr::select(c(extra_cols)) %>%
      sf::st_drop_geometry()
    x <- x %>%
      dplyr::select(-c(time_column_name)) %>%
      tidyr::pivot_wider(
        names_from = 'lag_amount',
        values_from = cols_to_get_vals_from,
        names_glue={"{.value}{ifelse(lag_amount != '', '_', '')}{lag_amount}"} # don't have an extra '_' at end if there was no lag for that row
      )

    if (length(extra_cols) > 0) {
      # add back any extra columns not used in the pivot_wider
      extra_col_vals <- extra_col_vals %>% dplyr::select(
        colnames(extra_col_vals)[!(colnames(extra_col_vals) %in% colnames(x))]
      )
      x <- x %>% dplyr::bind_cols(
        extra_col_vals
      )
    }

    x_colnames <- colnames(x)
    colnames(x)[x_colnames == 'envfetch__original_time_column'] <- time_column_name
  }
  # remove columns with all NA values
  x <- x %>%
    dplyr::select(dplyr::where(function(x) any(!is.na(x))))

  if (!is.na(out_filename)) {
    out_path <- file.path(out_dir, out_filename)
    if (verbose)
      cli::cli_alert_info(cli::col_black(paste('Saving to shapefile at', out_path)))
    if (overwrite && file.exists(out_path)) {
      file.remove(out_path)
    }
    if (getFileExtension(out_path) == 'csv') {
      # user probably wants x and y coordinates and this normally stuffs up with
      # csv as commas are used in the default output geometry column
      sf::st_write(x, out_path, append=ifelse(overwrite, FALSE, NA), layer_options = "GEOMETRY=AS_XY")
    } else {
      sf::st_write(x, out_path, append=ifelse(overwrite, FALSE, NA))
    }
    if (verbose)
      cli::cli_alert_info(
        cli::col_black(
          paste0(
            'Check the output directory (',
            out_dir,
            ') for your saved shapefile with your extracted x.'
          )
        )
      )
  }

  if (verbose)
    cli::cli_h1(cli::col_black('🐩 Fetched'))

  return(x)
}

getFileExtension <- function(file) {
  splt <- strsplit(file, ".", fixed=T)[[1]][]
  return(splt[length(splt)])
}
