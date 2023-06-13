#' Create time lagged rows (filled with NA values) using the 'time_column' column
#' as the starting time.
#'
#' @param x Your original starting tibble.
#' @param n_lags The number of time lags
#' @param time_lag The size of the time lag in a duration (e.g. `days()`)
#' @param lag_amount_units A lubridate object or a numeric in seconds that
#' determines the lag_amount column's unit.
#' @param timezone The timezone for dates
#'
#' @return A tibble with additional time-lagged rows
#'
#' @examples
create_time_lags <- function(
    x,
    n_lag_range=c(1, 14),
    time_lag=days(14),
    lag_amount_units=days(1),
    timezone='Australia/Perth'
) {
  geometries_to_lag <- x$geometry
  times_to_lag <- x$time_column

  new_geometries_column <- sf::st_sfc(crs=sf::st_crs(sf::st_geometry(x)))
  original_time_column <- lubridate::interval()
  new_time_column <- lubridate::interval()
  lag_amount <- c()

  for (lag in n_lag_range[1]:n_lag_range[2]) {
    max <- time_lag * lag
    min <- time_lag * (lag-1)
    new_time_column <- c(
      new_time_column,
      lubridate::interval(
        start=lubridate::int_end(times_to_lag) + min,
        end=lubridate::int_end(times_to_lag) + max,
        tz=timezone
      )
    )
    new_geometries_column <- c(new_geometries_column, geometries_to_lag)
    original_time_column <- c(original_time_column, times_to_lag)
    lag_amount <- c(
      lag_amount,
      rep(
        paste(
          as.numeric(lubridate::as.period(min)) / (as.numeric(lubridate::days(1))),
          '-',
          as.numeric(lubridate::as.period(max)) / (as.numeric(lubridate::days(1))),
          sep='_'
        ),
        length(times_to_lag)
      )
    )
  }

  x$original_time_column <- x$time_column
  x$lag_amount <- ''


  d <- x %>% dplyr::add_row(
    geometry = new_geometries_column,
    time_column = new_time_column,
    original_time_column = original_time_column,
    lag_amount = lag_amount
  )

  return(d)
}
