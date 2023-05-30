#' Create a time_rep object
#'
#' @param interval The datetime interval. Should be a lubridate period, e.g.
#' `lubridate::days(14)`.
#' @param n_before The number of times this interval should be repeated before the original datetime.
#' @param n_after The number of times this interval should be repeated after the original datetime.
#'
#' @return
#' @export
#'
#' @examples
time_rep <- function(interval, n_before=0, n_after=0) {
  stopifnot(lubridate:::is.period(interval))
  stopifnot(n_before %% 1 == 0)
  stopifnot(n_after %% 1 == 0)

  if (interval <= 0)
    stop(
      paste0(
        '`interval` must be a positive value. If you want to repeat times ',
        'going back in time, set `n_before` to a negative value.'
      )
    )

  return(
    list(
      interval=interval,
      n_before=n_before,
      n_after=n_after
    )
  )
}
