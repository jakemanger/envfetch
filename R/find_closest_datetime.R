get_closest_time_index_before <- function(dates, x) {
  return(which(abs(dates[dates < x]-x) == min(abs(dates[dates < x] - x))))
}

get_closest_time_index_after <- function(dates, x) {
  return(which(abs(dates[dates > x]-x) == min(abs(dates[dates > x] - x))))
}

get_closest_time_index <- function(dates, x) {
  return(which(abs(dates-x) == min(abs(dates - x))))
}

#' Find the closest datetime in a vector
#'
#' Finds the closest matching datetime before (before=TRUE), after (before=FALSE) or either (default or if before=NA)
#'
#' @param dates a vector of dates or datetimes
#' @param x a single date or datetime
#' @param before Leave as NA (the default) to find the closest or specify a boolean if the closest datetime should be before x (if TRUE) or after x (if FALSE)
#'
#' @return A logical vector of length dates.
#' @export
find_closest_datetime <- function(dates, x, before=NA) {
  dates <- lubridate::as_datetime(dates)
  x <- lubridate::as_datetime(x)

  if (is.na(before)) {
    return(get_closest_time_index(dates, x))
  }

  if (before) {
    return(get_closest_time_index_before(dates, x))
  } else {
    return(get_closest_time_index_after(dates, x))
  }
}
