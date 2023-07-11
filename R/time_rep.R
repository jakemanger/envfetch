#' Creates a `time_rep` object for repeating time intervals
#'
#' The `time_rep` function is used by the `fetch` function internally. It generates a list object with specified time intervals, and settings for repeated sampling
#' of environmental data before and after an original datetime. The function validates input parameters to ensure that they
#' are of the correct types and meet certain conditions.
#'
#' @param interval A lubridate period indicating the time interval to repeat. It should be positive, e.g. `lubridate::days(14)`.
#' @param n_start An integer indicating the number of steps before the original datetime from which the interval should start repeating. Default is -1.
#' @param n_end An integer indicating the number of steps after the original datetime to which the interval should continue repeating. Default is 0.
#'
#' @return A list object with three elements: `interval`, `n_start`, and `n_end`.
#' @export
#'
#' @examples
#' # Generates a time_rep object for a 14-day interval, starting 14 days before the original datetime and ending at the original datetime.
#' time_rep(lubridate::days(14))
#' # Generate a time_rep object for multiple 14-day intervals, between 28 periods before and ending 42 periods after the original datetime
#' time_rep(lubridate::days(14), -2, 3)
#'
time_rep <- function(interval, n_start=-1, n_end=0) {
  stopifnot(lubridate:::is.period(interval))
  stopifnot(n_start %% 1 == 0)
  stopifnot(n_end %% 1 == 0)

  if (n_start == 0 && n_end == 0) {
    stop(
      'Setting `n_start` and `n_end` to 0 will have no effect. Repeats that fall between these two multiples of the `interval` will be generated.'
    )
  }

  if (interval <= 0)
    stop(
      paste0(
        '`interval` must be a positive value. If you want to repeat times ',
        'going back in time, set `n_start` to a negative value.'
      )
    )

  return(
    list(
      interval=interval,
      n_start=n_start,
      n_end=n_end
    )
  )
}
