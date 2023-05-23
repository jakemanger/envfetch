#' Fetch data for each row in your dataset using the supplied functions
#'
#' Used to gather all input data necessary for modelling
#'
#' @param points A tibble with three columns: "x", "y"(
#' GPS points in lat and long EPSG 4326) and "time_column" (an interval),
#'
#' @return
#' @export
#'
#' @examples
fetch <- function(
    points,
    ...,
    use_cache=TRUE,
    out_dir='./output/',
    cache_dir=paste0(out_dir, 'cache/'),
    .time_rep=NA
) {
  print('Fetching your data... 🥏 🐕')

  if (!dir.exists(out_dir)) dir.create(out_dir)
  if (use_cache && !dir.exists(cache_dir)) dir.create(cache_dir)

  col_names_used_in_func <- c('time_column', 'geometry')

  if (!is.na(.time_rep)) {
    # generate all time lag intervals we want to extract data for
    points <- points %>%
      create_time_lags(n_lags=.time_rep$n, time_lag=.time_rep$interval) %>%
      distinct()
    col_names_used_in_func <- c(col_names_used_in_func, 'original_time_column', 'lag_amount')
  }

  grouping_variables <- colnames(points)
  grouping_variables <- grouping_variables[!(grouping_variables %in% col_names_used_in_func)]

  # create unique name to cache progress of extracted point data (so you can continue if you lose progress)
  hash <- rlang::hash(points)

  # capture the supplied as arguments
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
    # convert the formula to a function if it is a formula
    fun <- purrr::as_mapper(fun)

    # generate unique hash with dataset and function
    # to save progress
    # and either read cached output or run the function
    outpath <- paste0(cache_dir, hash, '_', rlang::hash(fun), '.rds')
    if (!use_cache || !file.exists(outpath)) {
      out <- fun(points)
      out <- out[,!(colnames(out) %in% colnames(points))]
      out <- sf::st_drop_geometry(out)
      saveRDS(out, outpath)
    } else {
      out <- readRDS(outpath)
    }
    return(out)
  })

  points <- dplyr::bind_cols(c(points, outs))

  # now take those time lagged points and set them as columns for their
  # original point
  if (!is.na(.time_rep)) {
    cols_to_get_vals_from <- colnames(points)[!(colnames(points) %in% c(grouping_variables, col_names_used_in_func))]
    points <- points %>%
      dplyr::select(-c(grouping_variables)) %>%
      tidyr::pivot_wider(
        names_from = c('lag_amount'),
        values_from = cols_to_get_vals_from
      )
  }
  # remove columns with all NA values
  points <- points %>%
    dplyr::select(dplyr::where(function(x) any(!is.na(x))))

  print(
    paste0(
      'Check the output directory (',
      out_dir,
      ') for csv files with your extracted points.'
    )
  )

  return(points)
}
