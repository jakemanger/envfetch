
parse_time_column <- function(
  d,
  time_column_name='time_column'
) {
  if (!(time_column_name %in% colnames(d)))
    stop(
      paste('The time column name', time_column_name, 'was not found in the input `points` dataset.')
    )

  time_column <- d %>% sf::st_drop_geometry() %>% dplyr::pull(time_column_name)

  if (lubridate::is.interval(time_column))
    return(time_column)

  if (all(is.character(time_column))) {
    if (stringr::str_detect(time_column, '--')) {
      # if you detect the format that lubridate saves to
      # but unfortunately can't load
      time_column <- time_column %>% string_to_interval()
      return(time_column)
    }

    # if it's just a datetime, load it as a datetime
    # and then convert to an interval from the start to the end of that date
    if (all(is.Date(time_column))) {
      dates <- lubridate::ymd(time_column)
      time_column <- lubridate::interval(dates, dates)
      return(time_column)
    }

    stop(paste(
      'The time column "',
      time_column_name,
      '" is not a lubridate::interval, not a interval string (datetimes seperated by "--") or a date (ymd format)',
      'Please convert it to one of these objects and try again.'
    ))
  }
}

string_to_interval <- function(time_column) {
  # we need to see if the datetime is an interval and if so, convert it to
  # a lubridate interval
  date_strings <- strsplit(time_column, "--")

  starts <- lapply(date_strings, `[[`, 1)
  ends <- lapply(date_strings, `[[`, 2)

  return(lubridate::interval(start = starts, end = ends))
}
