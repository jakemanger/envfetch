#' envfetch: Extract Environmental Data for Spatial-Temporal Objects
#'
#' `envfetch` extracts environmental data based on spatial-temporal inputs from local raster datasets or Google Earth Engine.
#' The function includes features for caching, memory management, and data summarisation. For extracting from multiple data
#' sources, specify the `r`, `bands` and `temporal_fun` parameters accordingly.
#'
#' @param x A tibble containing an `sf` "geometry" column, and optionally, a time column.
#' @param r Specifies the data source: either a local raster file path (which can include subdatasets) or a Google Earth Engine collection name. For multiple sources, provide a list and also specify the `bands` and `temporal_fun`, and optionally `time_column_name`, parameters accordingly.
#' @param bands Numeric or character vector specifying band numbers or names to extract. Use `NULL` to extract all bands. For multiple sources, provide a list of vectors.
#' @param temporal_fun Function or string used to summarize data for each time interval. Default is `mean(x, na.rm=TRUE)`. For Google Earth Engine, the string `'last'` returns the value closest to the start of the time interval. For multiple sources, provide a list of functions or strings.
#' @param spatial_fun Function or string used to summarize data spatially (if `x` is a polygon). Default (`'mean'`) for local files is `mean(x, na.rm=TRUE)` and for google earth engine is `rgee::ee$Reducer$mean()`. For local files, use `NULL` to not summarise spatially before summarising temporally.  If you are extracting from google earth engine, you must specify a google earth engine reducer `rgee::ee$Reducer` function (e.g. `rgee::ee$Reducer$sum()`). See https://r-spatial.github.io/rgee/reference/ee_extract.html". For different behaviour with multiple sources, provide a list of functions or strings.
#' @param use_cache Logical flag indicating whether to use caching. Default is `TRUE`.
#' @param out_dir Output directory for files. Default is `./output/`.
#' @param out_filename Name for the output file, defaulting to a timestamped `.gpkg` file.
#' @param overwrite Logical flag to overwrite existing output files. Default is `TRUE`.
#' @param cache_dir Directory for caching files. Default is `./output/cache/`.
#' @param time_column_name Name of the time column in `x`. Use `NULL` to auto-select a time column of type `lubridate::interval`. Default is NULL.
#' @param .time_rep Specifies repeating time intervals for extraction. Default is `NA`.
#' @param ... Additional arguments for underlying extraction functions.
#'
#' @details
#' `envfetch` serves as a high-level wrapper for specific data extraction methods:
#' - For local raster files, it employs either `extract_over_space` or `extract_over_time`.
#' - For Google Earth Engine collections, it uses `extract_gee`.
#' It also supports caching, allowing you to avoid repeated calculations and
#' resume work after interruptions.
#'
#' @return
#' An enhanced version of the input `sf` collection, `x`, augmented with the extracted environmental data.
#'
#' @examples
#' \dontrun{
#' # local raster file path example
#' extracted_data <- envfetch(x = my_data, r = "/path/to/local/raster/file.tif")
#'
#' # loaded raster object example
#' library(terra)
#' r <- rast("/path/to/local/raster/file.tif")
#' extracted_data <- envfetch(x = my_data, r = r)
#'
#' # Google Earth Engine example
#' extracted_gee_data <- envfetch(
#'   x = my_data,
#'   r = "GEE_COLLECTION_NAME",
#'   bands = c('BAND_NAME_1', 'BAND_NAME_2'),
#'   time_column_name = "time"
#' )
#'
#' # multiple data sources example (both local raster and Google Earth Engine)
#' extracted_multi_data <- envfetch(
#'   x = my_data,
#'   r = list("/path/to/local/raster/file1.tif", "GEE_COLLECTION_NAME1", "/path/to/local/raster/file2.tif"),
#'   bands = list(c(1, 2), c('BAND_NAME_1', 'BAND_NAME_2'), c(3, 4)),
#'   temporal_fun = list(mean, 'last', median),
#'   time_column_name = "time"
#' )
#' }
#'
#' @seealso
#' Other relevant functions, used internally by `envfetch`: \code{\link{fetch}}, \code{\link{extract_gee}}, \code{\link{extract_over_time}}
#'
#' @export
envfetch <- function(
  x,
  r=NULL,
  bands=NULL,
  temporal_fun='mean',
  spatial_fun='mean',
  use_cache=TRUE,
  out_dir=file.path('./output/'),
  out_filename=NA,
  overwrite=TRUE,
  cache_dir=file.path(out_dir, 'cache/'),
  time_column_name=NULL,
  .time_rep=NA,
  initialise_gee=TRUE,
  use_gcs=FALSE,
  use_drive=FALSE,
  ...
) {

  if (inherits(r, 'list')) {
    num_rasters <- length(r)
  } else {
    num_rasters <- 1
    r <- list(r)
  }

  bands <- parse_input(bands, num_rasters)
  temporal_fun <- parse_input(temporal_fun, num_rasters)
  spatial_fun <- parse_input(spatial_fun, num_rasters)

  functions <- c()

  for (i in 1:num_rasters) {

    if (file.exists(r[[i]])) {
      # if the user provides a file path, load the raster
      if (is.null(bands[[i]])) {
        subds=0
      } else {
        subds=bands[[i]]
      }
      r[[i]] <- terra::rast(r[[i]], subds)
    }
    if (inherits(r[[i]], "SpatRaster")) {

      if (!is.null(attr(spatial_fun[[i]], "class")) && attr(spatial_fun[[i]], "class") == "ee.Reducer") {
        stop("The provided spatial_fun for local file extraction is a rgee::ee$Reducer object. Use a function like mean instead.")
      }

      if (spatial_fun[[i]] == "mean") {
        spatial_fun[[i]] <- function(x) { mean(x, na.rm=TRUE) }
      } else if (spatial_fun[[i]]== "sum") {
        spatial_fun[[i]] <- function(x) { sum(x, na.rm=TRUE) }
      }

      if (temporal_fun[[i]] == 'mean') {
        temporal_fun[[i]] <- function(x) { rowMeans(x, na.rm=TRUE) }
      } else if (temporal_fun[[i]] == 'sum') {
        temporal_fun[[i]] <- function(x) { rowSums(x, na.rm=TRUE) }
      }

      functions <- c(
        functions,
        create_extract_over_time_function(i, r, temporal_fun, spatial_fun, ...)
      )
    } else {

      if (initialise_gee) {
        rgee::ee_Initialize(gcs = use_gcs, drive = use_drive)
        initialise_gee = FALSE
      }

      if (!is.null(attr(spatial_fun[[i]], "class")) && !any(attr(spatial_fun[[i]], "class") == "ee.Reducer")) {
        stop("The provided spatial_fun for google earth engine is not an rgee::ee$Reducer object. Use a reducer function like rgee::ee$Reducer$mean() instead.")
      }

      if (spatial_fun[[i]] == "mean") {
        spatial_fun[[i]] <- rgee::ee$Reducer$mean()
      } else if (spatial_fun[[i]] == "sum") {
        spatial_fun[[i]] <- rgee::ee$Reducer$sum()
      }

      if (temporal_fun[[i]] == 'mean') {
        temporal_fun[[i]] <- function(x) { mean(x, na.rm=TRUE) }
      } else if (temporal_fun[[i]] == 'sum') {
        temporal_fun[[i]] <- function(x) { sum(x, na.rm=TRUE) }
      }

      functions <- c(
        functions,
        create_extract_gee_function(i, r, bands, temporal_fun, spatial_fun, ...)
      )
    }
  }

  x <- fetch(
    x = x,
    ... = functions,
    use_cache = use_cache,
    out_dir = out_dir,
    out_filename = out_filename,
    overwrite = overwrite,
    cache_dir = cache_dir,
    time_column_name = time_column_name,
    .time_rep = .time_rep
  )

  return(x)
}

parse_input <- function(input, num_rasters) {
  input_name <- deparse(substitute(input))
  input_length <- length(input)
  input_is_list <- inherits(input, 'list')

  if (input_is_list && input_length == num_rasters) {
    # the user provided the required list
    input <- input
  } else if (is.null(input) || (!input_is_list && input_length == 1)) {
    # the user provided a single item which needs to be repeated
    # in a list
    input <- lapply(1:num_rasters, function(x) input)
  } else {
    # there is a mistake in the input
    stop(
      paste("Invalid '", input_name ,"' argument: \n"),
      paste("- If '", input_name, "' is a list, its length must equal the number of rasters in 'r'.\n"),
      paste("- If '", input_name, "' is not a list, it must contain only one element (function or string).\n"),
      paste("- If you don't specify '", input_name, "', or set it to NULL, default functions will be applied to each raster in 'r'.\n"),
      ifelse(input_name=='bands', "- If you are extracting from google earth engine, you must specify a google earth engine reducer `rgee::ee$reducer` function (e.g. `rgee::ee$reducer$mean()`). See https://r-spatial.github.io/rgee/reference/ee_extract.html\n", "")
    )
  }
  return(input)
}

create_extract_over_time_function <- function(i, r, temporal_fun, spatial_fun, ...) {
  force(i)
  force(r)
  force(temporal_fun)
  force(spatial_fun)

  ~extract_over_time(
    x = .x,
    r = r[[i]],
    temporal_fun = temporal_fun[[i]],
    spatial_fun = spatial_fun[[i]],
    ...
  )
}

create_extract_gee_function <- function(i, r, bands, temporal_fun, spatial_fun, ...) {
  force(i)
  force(r)
  force(bands)
  force(temporal_fun)
  force(spatial_fun)

  ~extract_gee(
    x = .x,
    collection_name = r[[i]],
    bands = bands[[i]],
    temporal_fun = temporal_fun[[i]],
    ee_reducer_fun = spatial_fun[[i]],
    initialise_gee=FALSE,
    ...
  )
}
