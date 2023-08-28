#' Extract Values from a Raster Layer over a space
#'
#' This function extracts values from a raster layer (r) over a spatial object (x).
#' If the spatial object contains multiple z indices (e.g. time), then spatial extractions
#' for each time will be returned.
#' It also ensures that the extraction does not exceed available RAM. If the raster is too large, the
#' function can chunk the raster into smaller pieces and process each chunk sequentially to
#' avoid memory overflow.
#'
#' @param x An sf spatial object, representing the locations over which raster values need to be extracted.
#' @param r A `terra::SpatRaster` object, representing the raster layer from which values need to be extracted.
#' @param chunk Logical. If `TRUE`, the raster will be split into chunks based on available RAM and processed
#'              chunk by chunk. If `FALSE`, the raster will be processed as a whole. Default is `TRUE`.
#' @param fun Function used to summarise multiple values within a polygon or line. Is passed to `extraction_fun`
#' internally. Defaults to `mean`.
#' @param na.rm Whether to remove NA values when summarising with the `fun` function.
#' @param ... Additional arguments to pass to `terra::extract`.
#' @param extraction_fun The extraction function to use. Default is `terra::extract`.
#'
#' @return A matrix or list where each column corresponds to a raster layer and each row corresponds to a
#'         geometry in `x`. The values represent the raster values at each point's location.
#'
#' @details
#' This function uses the `terra::mem_info` function to assess the RAM requirements for the extraction.
#' If chunking is required (i.e., the raster data does not fit in available RAM), the raster is divided
#' into a number of chunks such that each chunk's RAM requirement does not exceed the available RAM.
#' The function then processes each chunk sequentially, extracting the raster values over the spatial
#' object, and combines the results at the end.
#' Progress of the extraction is displayed using the `progressr` package.
#'
#' @examples
#' # Assuming 'some_raster' is a terra::SpatRaster object and 'some_sp' is a spatial object:
#' # result <- extract_over_space(x=some_sp, r=some_raster)
#' @export
extract_over_space <- function(x, r, fun=mean, na.rm=TRUE, chunk=TRUE, max_ram_frac_per_chunk=0.3, extraction_fun=terra::extract, ...) {
  mem_info_func <- purrr::quietly(terra::mem_info)
  mem_info <- mem_info_func(r)$result
  ram_required <- mem_info['needed']
  ram_available <- mem_info['available'] * 0.9
  ram_available_per_chunk <- ram_available * max_ram_frac_per_chunk

  message(
    paste(
      ram_required,
      'Kbs of RAM is required for extraction and',
      ram_available,
      'Kbs of RAM is available and ',
      ram_available_per_chunk,
      'Kbs of RAM is available per chunk'
    )
  )

  if (identical(extraction_fun, exactextractr::exact_extract)) {
    if (any(sf::st_geometry_type(x) == "POINT")) {
      stop("POINT detectd in input geometry. exactextractr::exact_extract only works with polygons. Use terra::extract instead.")
    }
  }

  if (chunk == TRUE && ram_required > ram_available_per_chunk) {
    # split raster into chunks based on available RAM
    times <- terra::time(r)

    num_chunks <- ceiling(ram_required / ram_available_per_chunk)
    chunk_size <- ceiling(length(times) / num_chunks)
    message(paste('Splitting job into', num_chunks, 'chunks'))

    progressr::with_progress({
      p <- progressr::progressor(steps = num_chunks)

      # initialize list to hold chunks
      r_chunks <- vector("list", num_chunks)

      # divide raster into chunks
      for (i in seq_len(num_chunks)) {
        start_index <- ((i - 1) * chunk_size) + 1
        end_index <- min(i * chunk_size, length(times))
        r_chunks[[i]] <- r[[start_index:end_index]]
      }

      extractions <- lapply(r_chunks, function(chunk) {
        ex <- extraction_fun(x = chunk, y = x, fun=fun, na.rm=na.rm, ...)
        # update progress bar after each extraction
        p()
        return(ex)
      })
      # perform extraction on each chunk and combine results
      extracted <- do.call(cbind, extractions)
    })
  } else {
    # perform extraction normally if raster fits in RAM
    extracted <- extraction_fun(x = r, y = x, fun=fun, na.rm=na.rm, ...)
  }
  message('Completed extraction')

  return(extracted)
}
