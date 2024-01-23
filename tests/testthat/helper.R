create_test_d <- function(polygons=FALSE, n=4, cellsize=1) {
  d <- throw(
    offset=c(119.625, -30.775),
    cellsize=cellsize,
    n=n,
    time_interval=lubridate::interval(start='2018-01-03', end='2018-01-03'),
  )
  d$time_column[1:2] <- lubridate::interval(start='2018-01-02', end='2018-01-03')

  if (polygons) {
    d <- d %>% sf::st_buffer(10)
  }

  return(d)
}

load_test_raster <- function() {
  return(
    terra::rast(system.file('testdata', 'test_tmin.nc', package='envfetch'))
  )
}
