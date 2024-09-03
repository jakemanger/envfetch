# skip on cran and ci without environmental variables for rgee testing
skip_on_cran()
skip_on_ci()


test_that("correct_results_returned_last", {
  point1 <- sf::st_point(c(115.798, -31.95)) # uwa
  point2 <- sf::st_point(c(131.034571, -25.345269)) # uluru
  points <- sf::st_sfc(point1, point2, point1, point2)
  points <- sf::st_sf(
    points,
    time=lubridate::ymd(
      c('2020-01-01', '2020-01-01', '2021-01-01', '2021-01-01'),
      tz='Australia/Perth'
    ),
    crs=4326
  )

  rgee::ee_Initialize()

  out <- points %>% fetch(
    ~extract_gee(
      .x,
      collection_name='MODIS/061/MOD13Q1',
      bands=c('NDVI', 'DetailedQA'),
      initialise_gee=FALSE,
      temporal_fun='last'
    ),
    time_column_name='time',
    out_filename=NA,
    use_cache=FALSE
  )

  # data provided by NASA's AppEEARS: https://appeears.earthdatacloud.nasa.gov/task/point
  correct_ndvis <- c(4381, 1062, 4463, 1177)
  expect_equal(
    as.vector(out$NDVI),
    correct_ndvis
  )
})


test_that("correct_results_returned_mean", {
  point1 <- sf::st_point(c(115.798, -31.95)) # uwa
  point2 <- sf::st_point(c(131.034571, -25.345269)) # uluru
  points <- sf::st_sfc(point1, point2, point1, point2)
  dates <- lubridate::ymd(
    c('2020-01-01', '2020-01-01', '2021-01-01', '2021-01-01'),
    tz='Australia/Perth'
  )
  points <- sf::st_sf(
    points,
    time=lubridate::interval(
      start=dates - lubridate::weeks(2),
      end=dates + lubridate::days(1)
    ),
    crs=4326
  )

  rgee::ee_Initialize()

  out <- points %>% fetch(
    ~extract_gee(
      .x,
      collection_name='MODIS/061/MOD13Q1',
      bands=c('NDVI', 'DetailedQA'),
      initialise_gee=FALSE,
      temporal_fun=mean,
    ),
    time_column_name='time',
    out_filename=NA,
    use_cache=FALSE
  )

  # data provided by NASA's AppEEARS: https://appeears.earthdatacloud.nasa.gov/task/point
  correct_mean_ndvis <- c(4229.5, 1139.5, 4463, 1155.5)
  expect_equal(
    out$NDVI,
    correct_mean_ndvis
  )
})

test_that("correct_results_returned_mean_not_lazy", {
  point1 <- sf::st_point(c(115.798, -31.95)) # uwa
  point2 <- sf::st_point(c(131.034571, -25.345269)) # uluru
  points <- sf::st_sfc(point1, point2, point1, point2)
  dates <- lubridate::ymd(
    c('2020-01-01', '2020-01-01', '2021-01-01', '2021-01-01'),
    tz='Australia/Perth'
  )
  points <- sf::st_sf(
    points,
    time=lubridate::interval(
      start=dates - lubridate::weeks(2),
      end=dates + lubridate::days(1)
    ),
    crs=4326
  )

  rgee::ee_Initialize()

  out <- points %>% fetch(
    ~extract_gee(
      .x,
      collection_name='MODIS/061/MOD13Q1',
      bands=c('NDVI', 'DetailedQA'),
      initialise_gee=FALSE,
      temporal_fun=mean,
      lazy=FALSE
    ),
    time_column_name='time',
    out_filename=NA,
    use_cache=FALSE
  )

  # data provided by NASA's AppEEARS: https://appeears.earthdatacloud.nasa.gov/task/point
  correct_mean_ndvis <- c(4229.5, 1139.5, 4463, 1155.5)
  expect_equal(
    out$NDVI,
    correct_mean_ndvis
  )
})


test_that("extracting_outside_data_range_errors", {
  point <- sf::st_point(c(115.798, -31.95))
  points <- sf::st_sfc(point)
  points <- sf::st_sf(
    points,
    time=lubridate::ymd('1995-10-04', tz='Australia/Perth'),
    crs=4326
  )

  rgee::ee_Initialize()

  expect_error(
    points %>% fetch(
      ~extract_gee(
        .x,
        collection_name='MODIS/061/MOD13Q1',
        bands=c('NDVI', 'DetailedQA'),
        initialise_gee=FALSE
      ),
      time_column_name='time',
      out_filename=NA,
      use_cache=FALSE
    ),
    'Maximum date of'
  )
  points$time <- lubridate::ymd('2200-01-01', tz='Australia/Perth')

  expect_error(
    points %>% fetch(
      ~extract_gee(
        .x,
        collection_name='MODIS/061/MOD13Q1',
        bands=c('NDVI', 'DetailedQA'),
        initialise_gee=FALSE
      ),
      time_column_name='time',
      out_filename=NA,
      use_cache=FALSE
    ),
    'Minimum date of'
  )
})
