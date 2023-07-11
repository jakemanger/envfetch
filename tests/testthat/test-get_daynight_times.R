context('get_daynight_times')

test_that("times_calculated_correctly", {
  point <- sf::st_point(c(115.798, -31.95))
  points <- sf::st_sfc(point)
  points <- sf::st_sf(points, time=lubridate::ymd('2023-07-11', tz='Australia/Perth'))

  out <- points %>% fetch(
    ~get_daynight_times(.x),
    time_column_name='time',
    out_filename=NA,
    use_cache=FALSE
  )

  expect_equal(out$time_since_sunrises, 16.6983333)
  expect_equal(out$time_since_sunsets, 6.5305556)
  expect_equal(out$day_hours, 10.1802778)
  expect_equal(out$night_hours, 13.8194444)
})
