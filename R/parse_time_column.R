parse_time_column <- function(
  d,
  time_column_name='time_column'
) {
  if (!(time_column_name %in% colnames(d)))
    stop(
      paste('The time column name', time_column_name, 'was not found in the input `points` dataset.')
    )

  time_column <- d %>% sf::st_drop_geometry() %>% dplyr::pull(time_column_name)
  check_for_na(time_column)

  if (lubridate::is.interval(time_column))
    return(time_column)

  if (is_date(time_column)) {
    # if it's just a date,
    # convert to an interval from the start to the end of that date
    time_column <- date_to_interval(time_column)
    return(time_column)
  }
  if (all(is.character(time_column))) {
    if (any(stringr::str_detect(time_column, '--'))) {
      # if you detect the format that lubridate saves to
      # but unfortunately can't load
      time_column <- time_column %>% string_to_interval()
      return(time_column)
    }

    # if it's just a date string, load it as a date
    # and then convert to an interval from the start to the end of that date
    if (is_date(time_column)) {
      warning('Loading date in UTC timezone, as no timezone was specified.')
      dates <- lubridate::ymd(time_column)
      time_column <- date_to_interval(dates)
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

check_for_na <- function(x) {
  if (any(is.na(x))) {
    indices <- which(is.na(lubridate::int_end(x)))
    stop(
      paste('NA values detected for the time column at index:', indices, '\n')
    )
  }
}

string_to_interval <- function(time_column) {
  # we need to see if the datetime is an interval and if so, convert it to
  # a lubridate interval
  date_strings <- strsplit(time_column, "--")

  starts <- unlist(lapply(date_strings, `[[`, 1))
  ends <- unlist(lapply(date_strings, `[[`, 2))

  return(lubridate::interval(start = starts, end = ends))
}


is_date <- function(x) {
  tryCatch({
    !any(is.na(lubridate::as_date(x)))
  }, warning = function(w) {
    FALSE
  }, error = function(e) {
    FALSE
  })
}

date_to_interval <- function(x) {
  time_column <- lubridate::interval(
    lubridate::floor_date(x),
    x + lubridate::days(1) - lubridate::seconds(1)
  )
}
