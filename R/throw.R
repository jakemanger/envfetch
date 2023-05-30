#' Create a sample of points for extracting data from. In other words, throw.
#'
#' @param offset
#' @param cellsize
#' @param n
#' @param time_interval
#' @param crs
#' @param what
#' @param square
#' @param flat_topped
#'
#' @return
#' @export
#'
#' @examples
throw <- function(
  offset = c(-180,-90),
  cellsize = c(10,10),
  n = c(36,18),
  time_interval,
  crs=sf::st_crs(4326),
  what='centers',
  square=TRUE,
  flat_topped=FALSE
) {
  stopifnot(lubridate::is.interval(time_interval))

  geometries <- sf::st_make_grid(
    offset=offset,
    cellsize=cellsize,
    n=n,
    what=what,
    square=square,
    flat_topped=flat_topped,
    crs=crs
  )

  times = rep(time_interval, each=length(geometries))
  points <- data.frame(time_column=times)
  sf::st_geometry(points) <- geometries

  return(points)
}
