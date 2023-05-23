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
        difftime(period2_finish, period2_start, units='hours'),
        difftime(period2_finish, period1_start, units='hours'),
        difftime(period1_finish, period2_start, units='hours'),
        difftime(period1_finish, period1_start, units='hours')
      )),
      0
    )
  )
}

get_time_since_sunrise <- function(
    start,
    lat,
    lon
) {
  start_date <- as_date(start)
  times <- getSunlightTimes(start_date, lat=lat, lon=lon, tz='UTC')
  return(difftime(start, times$sunrise))
}

get_time_since_sunset <- function(
    start,
    lat,
    lon
) {
  start_date <- as_date(start)
  get
  times <- getSunlightTimes(start_date, lat=lat, lon=lon, tz='UTC')
  return(difftime(start, times$sunset))
}

get_day_night_hours <- function(
    start,
    finish,
    lat,
    lon
) {
  start_date <- as_date(start)
  finish_date <- as_date(finish)
  dates <- seq(as.Date(start_date), as.Date(finish_date)+1, by='day')

  nights <- list()
  days <- list()
  for (i in 1:length(dates)) {
    date <- dates[i]
    # get the sunrise and sunset times for those days
    yesterday_times <- getSunlightTimes(date-1, lat=lat, lon=lon, tz='UTC')
    times <- getSunlightTimes(date, lat=lat, lon=lon, tz='UTC')
    new_night <- time_in_period(start, finish, yesterday_times$sunset, times$sunrise)
    new_day <- time_in_period(start, finish, times$sunrise, times$sunset)
    nights <- append(nights, new_night)
    days <- append(days, new_day)
  }

  return(unlist(list(nights=nights, days=days)))
}


#' Get day and night time-related information
#'
#' Calculates the time since sunrise, time since sunset and day and night hours
#' for your points `sf` object
#'
#' @param points An `sf` object that contains a geometry and a column with the
#' datetime as a `lubridate::interval`: `time_column`
#' @param savepath The path to save your extracted day and night time information
#' as an rds file.
#'
#' @return the input `points` object with day and night time-related information
#' column-binded.
#' @export
#'
#' @examples
get_daynight_times <- function(points, savepath='./output/extracted_day_night_stats.rds') {
  print('Calculating time since sunrise')
  pb <- dplyr::progress_estimated(nrow(points))
  time_since_sunrises <- 1:nrow(points) %>%
    map(
      function(x) {
        pb$tick()$print()
        start_time <- int_start((points$time_column[x]))
        coords <- st_coordinates(points$geometry)
        get_time_since_sunrise(
          start_time,
          lat=coords[2],
          lon=coords[1]
        )
      }
    )


  points <- points %>% bind_cols(time_since_sunrises=unlist(time_since_sunrises))

  print('Calculating time since sunset')
  pb <- progress_estimated(nrow(points))
  time_since_sunsets <- 1:nrow(points) %>%
    map(
      function(x) {
        pb$tick()$print()
        start_time <- int_start((points$time_column[x]))
        coords <- st_coordinates(points$geometry[x])
        get_time_since_sunset(
          start_time,
          lat=coords[2],
          lon=coords[1]
        )
      }
    )

  points <- points %>% bind_cols(time_since_sunsets=unlist(time_since_sunsets))

  print('Calculating day and night hours')
  pb <- progress_estimated(nrow(points))
  light_dark_minutes <- 1:nrow(points) %>%
    map(
      function(x) {
        pb$tick()$print()
        start_time <- int_start((points$time_column[x]))
        end_time <- int_end((points$time_column[x]))
        coords <- st_coordinates(points$geometry[x])
        get_day_night_hours(
          start_time,
          end_time,
          lat=coords[2],
          lon=coords[1]
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

  saveRDS(points, savepath)

  return(points)
}
