#' Extract Values from a Raster Layer over a space
#'
#' This function extracts values from a raster layer (x) over a spatial object (y).
#' If the spatial object contains multiple z indices (e.g. time), then spatial extractions
#' for each time will be returned.
#' It also ensures that the extraction does not exceed available RAM. If the raster is too large, the
#' function can chunk the raster into smaller pieces and process each chunk sequentially to
#' avoid memory overflow.
#'
#' @param x A `terra::SpatRaster` object, representing the raster layer from which values need to be extracted.
#' @param y A spatial object, representing the locations over which raster values need to be extracted.
#' @param chunk Logical. If `TRUE`, the raster will be split into chunks based on available RAM and processed
#'              chunk by chunk. If `FALSE`, the raster will be processed as a whole. Default is `TRUE`.
#'
#' @return A matrix or list where each column corresponds to a raster layer and each row corresponds to a
#'         point in `y`. The values represent the raster values at each point's location.
#'         If chunking was performed, the results from each chunk are combined column-wise.
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
#' # result <- extract_over_space(some_raster, some_sp)
#' @export
extract_over_space <- function(x, y, chunk=TRUE) {
  mem_info_func <- purrr::quietly(terra::mem_info)
  mem_info <- mem_info_func(x)$result
  ram_required <- mem_info['needed']
  ram_available <- mem_info['available']

  message(
    paste(
      ram_required,
      'Kbs of RAM is required for extraction and',
      ram_available,
      'Kbs of RAM is available'
    )
  )

  if (chunk == TRUE && ram_required > ram_available) {
    # split raster into chunks based on available RAM
    times <- terra::time(x)

    num_chunks <- ceiling(ram_required / ram_available)
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
        r_chunks[[i]] <- x[[start_index:end_index]]
      }


      extractions <- lapply(r_chunks, function(chunk) {
        ex <- terra::extract(x = chunk, y = y)
        # update progress bar after each extraction
        p()
        return(ex)
      })
      # perform extraction on each chunk and combine results
      extracted <- do.call(cbind, extractions)
    })
  } else {
    # perform extraction normally if raster fits in RAM
    extracted <- terra::extract(x = x, y = y)
  }
  message('Completed extraction')

  return(extracted)
}
