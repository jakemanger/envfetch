#' Fetch data from each row using anonymous functions
#'
#' This function passes your data through your supplied extraction functions,
#' caches progress, so that if your function crashes somewhere, you can continue
#' where you left off, shows progress and estimated time to completion and
#' allows you to repeat sampling across different times.
#'
#' @param x A tibble with a `sf` "geometry" and a column with time (a
#' `lubridate` interval or date), detected automatically or specified by the
#' `time_column_name` parameter.
#' @param ... Anonymous functions you would like to use on each row of the
#' dataset.
#' @param use_cache Whether to cache your progress. Allows you to continue where
#' you left off in case of an error or the process is interrupted. Also avoids
#' recomputing extractions between R sessions.
#' @param out_dir A directory to output your result. Is ignored if
#' out_filename = NA.
#' @param out_filename The path to output the result. Set to NA (the default) to
#' not save the result and only return the result.
#' @param overwrite Overwrite output file if exists.
#' @param cache_dir A directory to output cached progress. Is ignored if
#' use_cache = FALSE.
#' @param time_column_name Name of the time column in the dataset. If NULL
#' (the default), a column of type lubridate::interval is automatically
#' selected.
#' @param .time_rep A `time_rep` object. Used to repeat data extraction along
#' repeating time intervals before and after the original datetime. This can be
#' relative to the start or the end of the input time interval (specified by the
#' `relative_to_start` argument of `time_rep`). Defaults to the start.
#' @param batch_size The maximum number of rows or geometries to extract and
#' summarise at a time. Each batch will be cached to continue extraction in case
#' of interruptions. Larger batch sizes may result in overuse of rgee on the
#' server-side and hangs. Set `batch_size` to `1`, `NA` or `<1` for no batching.
#' Use `funs_to_use_batch_size` to define what functions batch_size will be
#' used with.
#' @param funs_to_use_batch_size A vector with the names of functions you want
#' to use batch_size for. Batch size is useful for some functions
#' (rgee: `'extract_gee'`) but not others (local: `'extract_over_time'`).
#' Defaults to `c('extract_gee')`.
#' @inheritDotParams extract_over_time -verbose
#' @inheritDotParams extract_gee -verbose
#'
#' @return tibble An augmented tibble with additional data fetched using
#' supplied functions.
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
#' # extract and summarise data every fortnight for the last six months
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
    .time_rep=NA,
    batch_size=200000,
    funs_to_use_batch_size=c('extract_gee')
) {
  # capture the supplied ... arguments as a list to preserve names
  args <- c(...)

  # preemptive garbage collection in case of previous run that got interrupted
  gc()

  verbose <- ifelse("verbose" %in% names(args), args$verbose, TRUE)

  if (verbose)
    cli::cli_h1(cli::col_black('\U0001F94F \U0001F415 Fetching your data'))

  if (!dir.exists(out_dir)) dir.create(out_dir)
  if (use_cache && !dir.exists(cache_dir)) dir.create(cache_dir)

  if (is.null(time_column_name)) {
    time_column_name <- find_time_column_name(x)
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
  x <- x %>% dplyr::select(-dplyr::any_of(c(extra_cols)))


  # split ... arguments and functions
  is_function <- sapply(args, function(x) {is.function(x) || purrr::is_formula(x)})
  if (!is.null(names(args))) {
    unnamed <- names(args) == '' | startsWith(names(args), '...') # in case an argument is a function
    is_function <- is_function & unnamed
  }

  not_funcs <- args[!is_function]
  funcs <- args[is_function]

  x <- x %>% dplyr::group_by(across(dplyr::all_of(c(time_column_name, geometry_column_name)))) %>%
    dplyr::mutate(envfetch__duplicate_ID = dplyr::cur_group_id()) %>%
    dplyr::ungroup()

  unique_x <- x[!duplicated(x$envfetch__duplicate_ID),]

  # calculate number of batches
  n_batches <- nrow(unique_x) / batch_size
  if (n_batches < 1 || is.infinite(n_batches) || is.na(n_batches))
    n_batches <- 1
  n_batches <- ceiling(n_batches)

  # loop through the supplied functions
  outs <- lapply(funcs, function(fun) {
    full_out <- data.frame()

    if (purrr::is_formula(fun)) {
      fun_string <- paste(as.character(fun), collapse='')
      # convert the formula to a function
      fun <- purrr::as_mapper(fun)
    } else {
      fun_string <- paste(deparse(fun), collapse='')
      fun_string <- gsub("\\s+", " ", fun_string)
    }

    # if not in funs_to_use_batch_size, don't do batching
    if (!grepl(paste(funs_to_use_batch_size, collapse='|'), fun_string)) {
      n_batches <- 1
    }

    if (n_batches > 1)
      cli::cli_alert_info(cli::col_blue('Splitting task into {n_batches} batches'))

    for (i in cli::cli_progress_along(1:n_batches, format='Extracting and summarising batch {cli::pb_current}')) {
      if (n_batches > 1) {
        # define start and end rows of batch
        start_i <- 1 + ((i-1) * batch_size)
        end_i <- i * batch_size
        if (i == n_batches)
          end_i <- nrow(unique_x)

        # get batch
        batch <- unique_x[start_i:end_i,]
      } else {
        batch <- unique_x
      }

      # create hash of input unique name to cache progress of extracted point data (so you can continue if you lose progress)
      input_hash <- rlang::hash(batch)

      if (use_cache) {
        outpath <- get_cache_path(fun, input_hash, fun_string, cache_dir)
      }

      if (!use_cache || (use_cache && !file.exists(outpath))) {
        if (verbose)
          cli::cli_alert_info(cli::col_blue(paste('Running', '{fun_string}')))

        # Use do.call to pass not_funcs as additional arguments to fun
        out <- do.call(fun, c(list(.x = batch), not_funcs))

        out <- out[,!(colnames(out) %in% colnames(batch))]
        out <- sf::st_drop_geometry(out)
        if (verbose)
          cli::cli_alert_success(cli::col_green(paste('\U0001F436 Completed', '{fun_string}')))

        if (use_cache)
          saveRDS(out, outpath)
      } else {
        if (verbose)
          cli::cli_alert_success(cli::col_green(paste('\U0001F573\UFE0F \U0001F9B4 Dug up cached result of', '{fun_string}')))
        out <- readRDS(outpath)
      }
    }

    full_out <- rbind(full_out, out)

    return(full_out)
  })

  unique_x <- dplyr::bind_cols(c(unique_x, outs))

  # add back duplicate rows using the data from the duplicate that was
  # used in the extraction
  cols_to_remove <- colnames(x)[!(colnames(x) %in% c('envfetch__duplicate_ID'))]
  x <- dplyr::left_join(
    x,
    unique_x %>% dplyr::select(-dplyr::any_of(cols_to_remove)),
    by = "envfetch__duplicate_ID"
  ) %>% dplyr::select(-dplyr::all_of(c('envfetch__duplicate_ID')))


  if (length(.time_rep) > 1) {
    # now take those time lagged x and set them as columns for their
    # original point
    cols_to_get_vals_from <- colnames(x)[!(colnames(x) %in% c(extra_cols, col_names_used_in_func))]
    x <- x %>%
      dplyr::select(-dplyr::any_of(c(time_column_name))) %>%
      tidyr::pivot_wider(
        names_from = 'lag_amount',
        values_from = cols_to_get_vals_from,
        names_glue={"{.value}{ifelse(lag_amount != '', '_', '')}{lag_amount}"} # don't have an extra '_' at end if there was no lag for that row
      )

    x_colnames <- colnames(x)
    colnames(x)[x_colnames == 'envfetch__original_time_column'] <- time_column_name
  }

  # add back any extra columns not used in the pivot_wider
  if (length(extra_cols) > 0) {
    extra_col_vals <- original_x %>% dplyr::select(dplyr::all_of(c(extra_cols))) %>%
      sf::st_drop_geometry()
    extra_col_vals <- extra_col_vals %>% dplyr::select(
      dplyr::all_of(
        colnames(extra_col_vals)[!(colnames(extra_col_vals) %in% colnames(x))]
      )
    )
    x <- x %>% dplyr::bind_cols(
      extra_col_vals
    )
  }
  # make sure column order is the same as input
  original_column_order <- colnames(original_x)
  current_column_order <- colnames(x)
  new_colnames <- current_column_order[!(current_column_order %in% original_column_order)]
  x <- x[,c(original_column_order, new_colnames)]

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
            ') for your saved vector data with your extracted x.'
          )
        )
      )
  }

  if (verbose)
    cli::cli_h1(cli::col_black('\U0001F429 Fetched'))

  # a bug check to make sure that rows weren't re-arranged
  if (!all(x$row_num == 1:nrow(x))) {
    stop("The 'row_num' column is not consecutive from 1 to the number of rows. Please submit a bug report to https://github.com/jakemanger/envfetch/issues/new")
  }
  # drop row_num as we don't need it after check above
  x <- x %>% dplyr::select(-dplyr::any_of(c('row_num')))

  return(x)
}

getFileExtension <- function(file) {
  splt <- strsplit(file, ".", fixed=T)[[1]][]
  return(splt[length(splt)])
}

get_cache_path <- function(fun, input_hash, fun_string, cache_dir) {
  # generate unique hash with function and dataset (input hash)
  # to save progress
  # and either read cached output or run the function
  function_env <- environment(fun)
  function_args <- sapply(ls(function_env), function(x) {
    out <- get(x, envir = function_env)
    is_function <- is.function(out) || purrr::is_formula(out)
    if (is_function) {
      # bytecode strings can mess up unique cache hashes the second time you run it
      # so we deparse these function arguments as strings and then make the hash
      # the function string with the function args to make our hash
      if ('signature' %in% names(out)) {
        # special case to support ee Reducer functions
        out <- out$signature$name
      } else {
        out <- deparse(out)
      }
    }
    return(out)
  })

  return(file.path(cache_dir, paste0(input_hash, '_', rlang::hash(capture.output(c(fun_string, function_args))), '.rds')))
}
