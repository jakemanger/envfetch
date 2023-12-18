#' get_daynight_times
#'
#' This function calculates time since sunrise, time since sunset, and day and
#' night hours for points in an `sf` object. The function also optionally saves
#' the output as an RDS file.
#'
#' @param points An `sf` object containing geometry and a time_column with
#' datetime as a `lubridate::interval`.
#' @param time_column_name Name of the time column in the dataset. If NULL (the
#' default), a column of type lubridate::interval is automatically selected.
#' @param save A logical value indicating whether to save the extracted day and
#' night time information as an RDS file. Default is `FALSE`.
#' @param savepath The path to save the RDS file, defaults to
#' './output/extracted_day_night_stats.rds'.
#' @param units A string specifying the units of time to return, defaults to
#' 'hours'.
#'
#' @return An `sf` object containing the original points and additional day and
#' night time-related information.
#'
#' @examples
#' \dontrun{
#' sf_with_times <- get_daynight_times(
#'   points_sf,
#'   save=TRUE,
#'   savepath='./daynight.rds', units='minutes'
#' )
#' }
#' @export
get_daynight_times <- function(
  points,
  time_column_name=NULL,
  save=FALSE,
  savepath='./output/extracted_day_night_stats.rds',
  units='hours'
) {
  if (is.null(time_column_name)) {
    time_column_name <- find_time_column_name(points)
  }

  if (!requireNamespace("suncalc", quietly = TRUE)) {
    stop('Package `suncalc` is required for calculating day and night times')
  }

  message('Calculating time since sunrise')
  pb <- cli::cli_progress_bar(nrow(points))
  time_since_sunrises <- 1:nrow(points) %>%
    purrr::map(
      function(x) {
        cli::cli_progress_update(id=pb)
        start_time <- sf::st_drop_geometry(points)[x,] %>% dplyr::pull(time_column_name) %>% lubridate::int_start()
        coords <- sf::st_coordinates(sf::st_geometry(points))
        get_time_since_sunrise(
          start_time,
          lat=coords[2],
          lon=coords[1],
          units=units
        )
      }
    )


  points <- points %>% dplyr::bind_cols(time_since_sunrises=unlist(time_since_sunrises))

  message('Calculating time since sunset')
  pb <- cli::cli_progress_bar(nrow(points))
  time_since_sunsets <- 1:nrow(points) %>%
    purrr::map(
      function(x) {
        cli::cli_progress_update(id=pb)
        start_time <- sf::st_drop_geometry(points)[x,] %>% dplyr::pull(time_column_name) %>% lubridate::int_start()
        coords <- sf::st_coordinates(sf::st_geometry(points)[x])
        get_time_since_sunset(
          start_time,
          lat=coords[2],
          lon=coords[1],
          units=units
        )
      }
    )

  points <- points %>% dplyr::bind_cols(time_since_sunsets=unlist(time_since_sunsets))

  message('Calculating day and night hours')
  pb <- cli::cli_progress_bar(nrow(points))
  light_dark_minutes <- 1:nrow(points) %>%
    purrr::map(
      function(x) {
        cli::cli_progress_update(id=pb)
        start_time <- sf::st_drop_geometry(points)[x,] %>% dplyr::pull(time_column_name) %>% lubridate::int_start()
        end_time <- sf::st_drop_geometry(points)[x,] %>% dplyr::pull(time_column_name) %>% lubridate::int_end()
        coords <- sf::st_coordinates(sf::st_geometry(points)[x])
        get_day_night_hours(
          start_time,
          end_time,
          lat=coords[2],
          lon=coords[1],
          units=units
        )
      }
    )

  out <- dplyr::bind_rows(light_dark_minutes)
  out[is.na(out)] <- 0

  night_hours <- rowSums(out[,grepl("night", names(out))])
  day_hours <- rowSums(out[,grepl("day", names(out))])

  day_night_hours <- tibble::tibble(
    day_hours=day_hours,
    night_hours=night_hours
  )

  points <- points %>% dplyr::bind_cols(day_night_hours)

  if (save)
    saveRDS(points, savepath)

  return(points)
}

#' calculate_daynight_times
#'
#' This function calculates the overlap in time, in specified units, between two
#' time periods defined by start and end times. It is specifically designed for
#' handling time overlaps related to day and night hours.
#'
#' @param period1_start A POSIXct object representing the start time of the first period.
#' @param period1_finish A POSIXct object representing the finish time of the first period.
#' @param period2_start A POSIXct object representing the start time of the second period.
#' @param period2_finish A POSIXct object representing the finish time of the second period.
#' @param units A string representing the units in which to output the time, defaults to 'hours'.
#'
#' @return A numeric representing the time overlap between two periods in specified units.
#'
#' @examples
#' \dontrun{
#' overlap_hours <- calculate_daynight_times(
#'   period1_start = as.POSIXct('2023-07-17 06:00:00'),
#'   period1_finish = as.POSIXct('2023-07-17 18:00:00'),
#'   period2_start = as.POSIXct('2023-07-17 12:00:00'),
#'   period2_finish = as.POSIXct('2023-07-17 20:00:00'),
#'   units = 'hours'
#' )
#' }
calculate_daynight_times <- function(
    period1_start,
    period1_finish,
    period2_start,
    period2_finish,
    units='hours'
) {
  return(
    max(
      as.numeric(min(
        difftime(period2_finish, period2_start, units=units),
        difftime(period2_finish, period1_start, units=units),
        difftime(period1_finish, period2_start, units=units),
        difftime(period1_finish, period1_start, units=units)
      )),
      0
    )
  )
}

#' get_time_since_sunrise
#'
#' This function calculates the time elapsed since sunrise given a specific location and time.
#'
#' @param start A POSIXct object representing the start time.
#' @param lat The latitude of the location.
#' @param lon The longitude of the location.
#' @param units A string representing the units in which to output the time, defaults to 'hours'.
#'
#' @return A numeric representing the time elapsed since sunrise.
#'
#' @examples
#' \dontrun{
#' time_since_sunrise <- get_time_since_sunrise(start = Sys.time(), lat = 51.5074, lon = 0.1278, units = 'minutes')
#' }
get_time_since_sunrise <- function(
    start,
    lat,
    lon,
    units='hours'
) {
  start_date <- lubridate::as_date(start)
  times <- suncalc::getSunlightTimes(start_date, lat=lat, lon=lon, tz='UTC')
  time_since_sunrise <- difftime(start, times$sunrise, units=units)
  if (time_since_sunrise < 0) {
    previous_day_times <- suncalc::getSunlightTimes(start_date - lubridate::days(1), lat=lat, lon=lon, tz='UTC')
    time_since_sunrise <- difftime(start, previous_day_times$sunrise, units=units)
  }
  return(time_since_sunrise)
}

#' get_time_since_sunset
#'
#' This function calculates the time elapsed since sunset given a specific location and time.
#'
#' @param start A POSIXct object representing the start time.
#' @param lat The latitude of the location.
#' @param lon The longitude of the location.
#' @param units A string representing the units in which to output the time, defaults to 'hours'.
#'
#' @return A numeric representing the time elapsed since sunset.
#'
#' @examples
#' \dontrun{
#' time_since_sunset <- get_time_since_sunset(start = Sys.time(), lat = 51.5074, lon = 0.1278, units = 'minutes')
#' }
get_time_since_sunset <- function(
    start,
    lat,
    lon,
    units='hours'
) {
  start_date <- lubridate::as_date(start)
  get
  times <- suncalc::getSunlightTimes(start_date, lat=lat, lon=lon, tz='UTC')
  time_since_sunset <- difftime(start, times$sunset, units=units)
  if (time_since_sunset < 0) {
    previous_day_times <- suncalc::getSunlightTimes(start_date - lubridate::days(1), lat=lat, lon=lon, tz='UTC')
    time_since_sunset <- difftime(start, previous_day_times$sunset, units=units)
  }
  return(time_since_sunset)
}

#' get_day_night_hours
#'
#' This function calculates the number of day and night hours for a given time range and location.
#'
#' @param start A POSIXct object representing the start time.
#' @param finish A POSIXct object representing the end time.
#' @param lat The latitude of the location.
#' @param lon The longitude of the location.
#' @param units A string representing the units in which to output the time, defaults to 'hours'.
#'
#' @return A list containing the number of day and night hours.
#'
#' @examples
#' \dontrun{
#' day_night_hours <- get_day_night_hours(start = Sys.time(), finish = Sys.time() + lubridate::hours(24), lat = 51.5074, lon = 0.1278, units = 'minutes')
#' }
get_day_night_hours <- function(
    start,
    finish,
    lat,
    lon,
    units='hours'
) {
  start_date <- lubridate::as_date(start)
  finish_date <- lubridate::as_date(finish)
  dates <- seq(as.Date(start_date), as.Date(finish_date)+1, by='day')

  nights <- list()
  days <- list()
  for (i in 1:length(dates)) {
    date <- dates[i]
    # get the sunrise and sunset times for those days
    yesterday_times <- suncalc::getSunlightTimes(date-1, lat=lat, lon=lon, tz='UTC')
    times <- suncalc::getSunlightTimes(date, lat=lat, lon=lon, tz='UTC')
    new_night <- time_in_period(start, finish, yesterday_times$sunset, times$sunrise, units=units)
    new_day <- time_in_period(start, finish, times$sunrise, times$sunset, units=units)
    nights <- append(nights, new_night)
    days <- append(days, new_day)
  }

  return(unlist(list(nights=nights, days=days)))
}

#' time_in_period
#'
#' This function calculates the overlap in time, in specified units, between two
#' time periods defined by start and end times.
#'
#' @param period1_start A POSIXct object representing the start time of the first period.
#' @param period1_finish A POSIXct object representing the finish time of the first period.
#' @param period2_start A POSIXct object representing the start time of the second period.
#' @param period2_finish A POSIXct object representing the finish time of the second period.
#' @param units A string representing the units in which to output the time, defaults to 'hours'.
#'
#' @return A numeric representing the time overlap between two periods in specified units.
#'
#' @examples
#' \dontrun{
#' overlap_hours <- time_in_period(
#'   period1_start = as.POSIXct('2023-07-17 06:00:00'),
#'   period1_finish = as.POSIXct('2023-07-17 18:00:00'),
#'   period2_start = as.POSIXct('2023-07-17 12:00:00'),
#'   period2_finish = as.POSIXct('2023-07-17 20:00:00'),
#'   units = 'hours'
#' )
#' }
time_in_period <- function(
    period1_start,
    period1_finish,
    period2_start,
    period2_finish,
    units='hours'
) {
  return(
    max(
      as.numeric(min(
        difftime(period2_finish, period2_start, units=units),
        difftime(period2_finish, period1_start, units=units),
        difftime(period1_finish, period2_start, units=units),
        difftime(period1_finish, period1_start, units=units)
      )),
      0
    )
  )
}
