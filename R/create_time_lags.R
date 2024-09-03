#' create_time_lags
#'
#' This function creates time-lagged rows in a tibble using the 'time_column'
#' as the reference point. The new rows are filled with NA values and have
#' their time interval shifted by a specified lag duration.
#'
#' @param x A tibble containing the original data.
#' @param n_lag_range A numeric vector of length 2 defining the range of lag
#' times.
#' @param time_lag A duration object specifying the size of the time lag.
#' @param lag_amount_units A duration object or a numeric in seconds that
#' determines the unit for the 'lag_amount' column.
#' @param relative_to_start A logical value indicating whether the lag should be
#' relative to the start or the end of the input time interval.
#' @param time_column_name Name of the time column in `x`.
#'
#' @return A tibble with the original data and additional time-lagged rows.
#'
#' @importFrom rlang :=
#'
#' @examples
#' \dontrun{
#' lagged_tibble <- create_time_lags(
#'   x = original_tibble,
#'   n_lag_range = c(1, 14),
#'   time_lag = lubridate::days(14),
#'   lag_amount_units = lubridate::days(1),
#'   relative_to_start = TRUE
#' )
#' }
create_time_lags <- function(
    x,
    n_lag_range=c(1, 14),
    time_lag=lubridate::days(14),
    lag_amount_units=lubridate::days(1),
    relative_to_start=TRUE,
    time_column_name='time_column'
) {
  geometries_to_lag <- sf::st_geometry(x)
  times_to_lag <- x %>% dplyr::pull(time_column_name)

  start_range <- seq(n_lag_range[1], n_lag_range[2], by = 1)
  start_range <- start_range[1:length(start_range)-1]

  n_rows_to_add <- length(start_range)

  new_geometries_column <- vector("list", n_rows_to_add)
  envfetch__original_time_column <- vector('list', n_rows_to_add)
  new_time_column <- vector('list', n_rows_to_add)
  envfetch__original_time_column <- vector('list', n_rows_to_add)
  lag_amount <- vector('list', n_rows_to_add)
  row_num <- vector('list', n_rows_to_add)

  if (!('row_num' %in% colnames(x))) {
    x$row_num <- 1:nrow(x)
    message('`row_num` was not found in the data, so adding it to easily link time lag rows with their original source row')
  }


  timezone <- lubridate::tz(x %>% dplyr::pull(time_column_name) %>% lubridate::int_start())


  if (relative_to_start) {
    relative_time <- lubridate::int_start(times_to_lag)
  } else {
    relative_time <- lubridate::int_end(times_to_lag)
  }

  i <- 1
  for (start in start_range) {
    min <- start * time_lag
    max <- (start + 1) * time_lag

    new_time_column[[i]] <- lubridate::interval(
      start=relative_time + min,
      end=relative_time + max - lubridate::seconds(1),
      tz=timezone
    )
    new_geometries_column[[i]] <- geometries_to_lag
    envfetch__original_time_column[[i]] <- times_to_lag
    lag_amount[[i]] <- rep(
      paste(
        abs(as.numeric(lubridate::as.period(min)) / (as.numeric(lubridate::days(1)))),
        abs(as.numeric(lubridate::as.period(max)) / (as.numeric(lubridate::days(1)))),
        sep='_'
      ),
      length(times_to_lag)
    )
    row_num[[i]] <- x$row_num

    i <- i + 1
  }

  x$envfetch__original_time_column <- x %>% dplyr::pull(time_column_name)
  x$lag_amount <- ''

  geometry_column_name = attr(x, "sf_column")

  x <- x %>% dplyr::add_row(
    !!geometry_column_name := do.call(c, new_geometries_column),
    !!time_column_name := do.call(c, new_time_column),
    envfetch__original_time_column = do.call(c, envfetch__original_time_column),
    lag_amount = unlist(lag_amount),
    row_num = unlist(row_num)
  )

  gc()

  return(x)
}
