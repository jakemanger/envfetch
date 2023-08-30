test_that("time lags correct minimum", {
  x <- create_test_d(polygons=FALSE)
  x$row_num <- 1:nrow(x)

  times_from_start <- 26
  interval <- lubridate::days(14)

  min_expected_time_column <- (
    min(lubridate::int_start(x$time_column))
    - (times_from_start * interval)
  )

  time_rep = time_rep(interval = interval, n_start = -times_from_start, n_end = 0)

  x <- x %>%
    create_time_lags(
      n_lag_range=c(time_rep$n_start, time_rep$n_end),
      time_lag=time_rep$interval,
      relative_to_start=time_rep$relative_to_start
    )

  expect_equal(
    min(lubridate::int_start(x$time_column)),
    min_expected_time_column
  )
})

test_that("time lags correct maximum", {
  x <- create_test_d(polygons=FALSE)

  times_from_start <- 26
  interval <- lubridate::days(14)

  max_expected_time_column <- (
    max(lubridate::int_end(x$time_column))
  )

  time_rep = time_rep(interval = interval, n_start = -times_from_start, n_end = 0)

  x <- x %>%
    create_time_lags(
      n_lag_range=c(time_rep$n_start, time_rep$n_end),
      time_lag=time_rep$interval,
      relative_to_start=time_rep$relative_to_start
    )

  expect_equal(
    max(lubridate::int_end(x$time_column)),
    max_expected_time_column
  )
})

test_that("time lags correct number of repeats", {
  x <- create_test_d(polygons=FALSE)

  times_from_start <- 26
  interval <- lubridate::days(14)

  expected_nrow <- nrow(x) * (times_from_start + 1)

  time_rep = time_rep(interval = interval, n_start = -times_from_start, n_end = 0)

  x <- x %>%
    create_time_lags(
      n_lag_range=c(time_rep$n_start, time_rep$n_end),
      time_lag=time_rep$interval,
      relative_to_start=time_rep$relative_to_start
    )

  expect_equal(
    nrow(x),
    expected_nrow
  )
})
