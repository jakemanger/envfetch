#' throw
#'
#' This function creates a sample of points for extracting data from a grid.
#' It generates a grid of points or polygons over a specified region, and
#' assigns each grid cell a specific time interval. The points can be used
#' for extracting data from raster or other spatial data sources.
#'
#' @param offset A numeric vector of length 2 indicating the offset from the
#' origin for the grid. Default is c(-180, -90).
#' @param cellsize A numeric vector of length 1 or 2 indicating the size of the
#' cells in the grid. Default is c(10, 10).
#' @param n A numeric vector of length 1 or 2 indicating the number of cells in
#' the grid in the x and y directions. Default is c(36, 18).
#' @param time_interval An interval object from the lubridate package,
#' indicating the time span for each point.
#' @param crs A coordinate reference system object from the sf package.
#' Default is sf::st_crs(4326), which is the WGS 84 geographic coordinate system.
#' @param what A string indicating what kind of geometries to return: 'centers'
#' for point geometries in the center of the cells, 'polygons' for polygon
#' geometries filling the cells. Default is 'centers'.
#' @param square A logical indicating whether to use square cells. Default is TRUE.
#' @param flat_topped A logical indicating whether to use flat-topped or
#' pointy-topped hexagon cells. Ignored if square is TRUE. Default is FALSE.
#'
#' @return An sf object with point geometries and an associated time column.
#'
#' @examples
#' \dontrun{
#' sample_points <- throw(
#'   offset = c(119.625, -30.775),
#'   cellsize = 1,
#'   n = 4,
#'   time_interval = lubridate::interval(
#'     start = lubridate::ymd("2018-01-01"),
#'     end = lubridate::ymd("2018-01-04")
#'   ),
#'   crs = sf::st_crs(4326),
#'   what = 'centers',
#'   square = TRUE,
#'   flat_topped = FALSE
#' )
#' }
#' @export
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
