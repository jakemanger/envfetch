#' Get day and night time-related information
#'
#' Calculates the time since sunrise, time since sunset and day and night hours
#' for your points `sf` object
#'
#' @param points An `sf` object that contains a geometry and a column with the
#' datetime as a `lubridate::interval`: `time_column`
#' @param save Whether to save extracted day and night time information
#' as an rds file.
#' @param savepath The path to save your extracted day and night time information
#' as an rds file.
#' @param units The units of time to return. Defaults to 'hours'.
#'
#' @return the input `points` object with day and night time-related information
#' column-binded.
#' @export
#'
#' @examples
get_daynight_times <- function(points, save=FALSE, savepath='./output/extracted_day_night_stats.rds', units='hours') {
  message('Calculating time since sunrise')
  pb <- dplyr::progress_estimated(nrow(points))
  time_since_sunrises <- 1:nrow(points) %>%
    map(
      function(x) {
        pb$tick()$print()
        start_time <- lubridate::int_start(points$time_column[x])
        coords <- sf::st_coordinates(sf::st_geometry(points))
        get_time_since_sunrise(
          start_time,
          lat=coords[2],
          lon=coords[1],
          units=units
        )
      }
    )


  points <- points %>% bind_cols(time_since_sunrises=unlist(time_since_sunrises))

  message('Calculating time since sunset')
  pb <- progress_estimated(nrow(points))
  time_since_sunsets <- 1:nrow(points) %>%
    map(
      function(x) {
        pb$tick()$print()
        start_time <- lubridate::int_start((points$time_column[x]))
        coords <- sf::st_coordinates(sf::st_geometry(points)[x])
        get_time_since_sunset(
          start_time,
          lat=coords[2],
          lon=coords[1],
          units=units
        )
      }
    )

  points <- points %>% bind_cols(time_since_sunsets=unlist(time_since_sunsets))

  message('Calculating day and night hours')
  pb <- progress_estimated(nrow(points))
  light_dark_minutes <- 1:nrow(points) %>%
    map(
      function(x) {
        pb$tick()$print()
        start_time <- lubridate::int_start((points$time_column[x]))
        end_time <- lubridate::int_end((points$time_column[x]))
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

  out <- bind_rows(light_dark_minutes)
  out[is.na(out)] <- 0

  night_hours <- rowSums(out[,grepl("night", names(out))])
  day_hours <- rowSums(out[,grepl("day", names(out))])

  day_night_hours <- tibble(
    day_hours=day_hours,
    night_hours=night_hours
  )

  points <- points %>% bind_cols(day_night_hours)

  if (save)
    saveRDS(points, savepath)

  return(points)
}


#' Calculate the time (in specified units) one time period overlaps with another
#'
#' @param period1_start The start datetime of the first period
#' @param period1_finish The finish datetime of the first period
#' @param period2_start The start datetime of the second period
#' @param period2_finish The finish datetime of the second period
#' @param units Time units to output. Defaults to 'hours'.
#'
#' @return Time in specified units where time periods overlap
#'
#' @examples
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

get_time_since_sunrise <- function(
    start,
    lat,
    lon,
    units='hours'
) {
  start_date <- as_date(start)
  times <- suncalc::getSunlightTimes(start_date, lat=lat, lon=lon, tz='UTC')
  time_since_sunrise <- difftime(start, times$sunrise, units=units)
  if (time_since_sunrise < 0) {
    previous_day_times <- suncalc::getSunlightTimes(start_date - lubridate::days(1), lat=lat, lon=lon, tz='UTC')
    time_since_sunrise <- difftime(start, previous_day_times$sunrise, units=units)
  }
  return(time_since_sunrise)
}

get_time_since_sunset <- function(
    start,
    lat,
    lon,
    units='hours'
) {
  start_date <- as_date(start)
  get
  times <- suncalc::getSunlightTimes(start_date, lat=lat, lon=lon, tz='UTC')
  time_since_sunset <- difftime(start, times$sunset, units=units)
  if (time_since_sunset < 0) {
    previous_day_times <- suncalc::getSunlightTimes(start_date - lubridate::days(1), lat=lat, lon=lon, tz='UTC')
    time_since_sunset <- difftime(start, previous_day_times$sunset, units=units)
  }
  return(time_since_sunset)
}

get_day_night_hours <- function(
    start,
    finish,
    lat,
    lon,
    units='hours'
) {
  start_date <- as_date(start)
  finish_date <- as_date(finish)
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

#' Calculate the time (in specified units) one time period overlaps with another
#'
#' @param period1_start The start datetime of the first period
#' @param period1_finish The finish datetime of the first period
#' @param period2_start The start datetime of the second period
#' @param period2_finish The finish datetime of the second period
#' @param units Time units to output. Defaults to 'hours'.
#'
#' @return Time in specified units where time periods overlap
#'
#' @examples
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
