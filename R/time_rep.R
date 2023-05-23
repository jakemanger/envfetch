#' Create a time_rep object
#'
#' @param interval The datetime interval. Should be a lubridate period, e.g.
#' `lubridate::days(14)`.
#'
#' @param n
#'
#' @return
#' @export
#'
#' @examples
time_rep <- function(interval, n) {
  stopifnot(lubridate:::is.period(interval))
  stopifnot(n %% 1 == 0)

  return(
    list(
      interval=interval,
      n=n
    )
  )
}
