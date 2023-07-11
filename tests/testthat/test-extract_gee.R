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
    'Minimum date of'
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
    'Maximum date of'
  )
})
