#' envfetch: Extract Environmental Data for Spatial-Temporal Objects
#'
#' `envfetch` extracts environmental data based on spatial-temporal inputs from local raster datasets or Google Earth Engine.
#' The function includes features for caching, memory management, and data summarization. For extracting from multiple data
#' sources, specify the `r`, `bands` and `temporal_fun` parameters accordingly.
#'
#' @param x A tibble containing an `sf` "geometry" column, and optionally, a time column.
#' @param r Specifies the data source: either a local raster file path (which can include subdatasets) or a Google Earth Engine collection name. For multiple sources, provide a list and also specify the `bands` and `temporal_fun`, and optionally `time_column_name`, parameters accordingly.
#' @param bands Numeric or character vector specifying band numbers or names to extract. Use `NULL` to extract all bands. For multiple sources, provide a list of vectors.
#' @param temporal_fun Function or string used to summarize data for each time interval. Default is `mean(x, na.rm=TRUE)`. For Google Earth Engine, the string `'last'` returns the value closest to the start of the time interval. For multiple sources, provide a list of functions or strings.
#' @param use_cache Logical flag indicating whether to use caching. Default is `TRUE`.
#' @param out_dir Output directory for files. Default is `./output/`.
#' @param out_filename Name for the output file, defaulting to a timestamped `.gpkg` file.
#' @param overwrite Logical flag to overwrite existing output files. Default is `TRUE`.
#' @param cache_dir Directory for caching files. Default is `./output/cache/`.
#' @param time_column_name Name of the time column in `x`. Use `NULL` for spatial-only extraction. Use `'auto'` to auto-select a time column of type `lubridate::interval`. Default is `'auto'`. For multiple sources where you want spatial-only extractions, provide a list and set the value/s you want spatial-only to `NULL`.
#' @param .time_rep Specifies repeating time intervals for extraction. Default is `NA`.
#' @param ... Additional arguments for underlying extraction functions.
#'
#' @details
#' `envfetch` serves as a high-level wrapper for specific data extraction methods:
#' - For local raster files, it employs either `extract_over_space` or `extract_over_time`.
#' - For Google Earth Engine collections, it uses `extract_gee`.
#' It also supports caching, allowing you to resume work after interruptions.
#'
#' @return
#' An enhanced version of the input tibble `x`, augmented with the extracted environmental data.
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
#' Other relevant functions: \code{\link{fetch}}, \code{\link{extract_gee}}, \code{\link{extract_over_time}}
#'
#' @export
envfetch <- function(
  x,
  r=NULL,
  bands=NULL,
  temporal_fun=function(x) { mean(x, na.rm=TRUE) },
  use_cache=TRUE,
  out_dir=file.path('./output/'),
  out_filename=paste0('output_', format(Sys.time(), "%Y_%m_%d_%H_%M_%S"), '.gpkg'),
  overwrite=TRUE,
  cache_dir=file.path(out_dir, 'cache/'),
  time_column_name='auto',
  .time_rep=NA,
  ...
) {

  # process rasters
  if (inherits(r, 'list')) {
    message('Detected list of rasters in `r`. Fetching data from multiple sources')
    num_rasters <- length(r)
  } else {
    num_rasters <- 1
  }

  functions <- c()

  for (i in 1:num_rasters) {
    if (inherits(r, 'list')) {
      r_i <- r[[i]]
      if (inherits(bands, 'list') && length(bands) == num_rasters) {
        bands_i <- bands[[i]]
      } else if (!inherits(bands, 'list') && length_bands == 1) {
        bands_i <- bands
      } else {
        stop("Invalid 'bands' argument: \n",
             "- If 'bands' is a list, its length must equal the number of rasters in 'r'.\n",
             "- If 'bands' is not a list, it must contain only one element.\n",
             "- If you don't specify 'bands', or set it to NULL, all bands will be used for each raster in 'r'.")
      }

      if (inherits(temporal_fun, 'list') && length(temporal_fun) == num_rasters) {
        temporal_fun_i <- temporal_fun[[i]]
      } else if (!inherits(temporal_fun, 'list') && length(temporal_fun) == 1) {
        temporal_fun_i <- temporal_fun
      } else {
        stop("Invalid 'temporal_fun' argument: \n",
             "- If 'temporal_fun' is a list, its length must equal the number of rasters in 'r'.\n",
             "- If 'temporal_fun' is not a list, it must contain only one element (function or string).\n",
             "- If you don't specify 'temporal_fun', or set it to NULL, default functions will be applied to each raster in 'r'.")
      }

      if (inherits(time_column_name, 'list') && length(time_column_name) == num_rasters) {
        time_column_name_i <- time_column_name[[i]]
      } else if (!inherits(time_column_name, 'list') && length(time_column_name) == 1) {
        time_column_name_i <- time_column_name
      } else {
        stop("Invalid 'time_column_name' argument: \n",
             "- If 'time_column_name' is a list, its length must equal the number of rasters in 'r'.\n",
             "- If 'time_column_name' is not a list, it must contain only one element (string or NULL).\n",
             "- If you don't specify 'time_column_name', or set it to 'auto', a time column will be automatically detected and used to summarise extracts over time for each raster in 'r'.")
      }
    } else {
      r_i <- r
      bands_i <- bands
      temporal_fun_i <- temporal_fun
      time_column_name_i <- temporal_fun
    }

    if (file.exists(r_i)) {
      if (is.null(bands)) {
        subds=0
      } else {
        subds=bands_i
      }

      r_i <- terra::rast(r_i, subds)
    }

    if (inherits(r, "SpatRaster")) {
      functions <- c(
        functions,
        ~extract_over_time(
          .x,
          r = r_i,
          bands = bands_i,
          time_column_name=time_column_name,
          temporal_fun = temporal_fun_i,
          ...
        ),
        time_column_name = time_column_name_i
      )
    } else {
      functions <- c(
        functions,
        ~extract_gee(
          .x,
          collection_name=r_i,
          bands=bands_i,
          time_column_name=time_column_name,
          temporal_fun = temporal_fun_i,
          ...
        ),
        time_column_name = time_column_name_i
      )
    }
  }

  params <- c(
    functions,
    use_cache=use_cache,
    out_dir=out_dir,
    out_filename=out_filename,
    overwrite=overwrite,
    cache_dir=cache_dir,
    time_column_name=time_column_name,
    .time_rep=.time_rep
  )

  x <- do.call(fetch, params)

  return(x)
}

