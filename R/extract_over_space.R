#' Extract Values from a Raster Layer over a space
#'
#' This function extracts values from a raster layer (r) over a spatial object
#' (x).
#' If the spatial object contains multiple z indices (e.g. time), then spatial
#' extractions for each time will be returned.
#' It also ensures that the extraction does not exceed available RAM. If the
#' raster is too large, the function can chunk the raster into smaller pieces
#' and process each chunk sequentially to avoid memory overflow.
#'
#' @param x An sf spatial object, representing the locations over which raster
#' values need to be extracted.
#' @param r A `terra::SpatRaster` object, representing the raster layer from
#' which values need to be extracted.
#' @param chunk Logical. If `TRUE`, the raster will be split into chunks based
#' on available RAM and processed chunk by chunk. If `FALSE`, the raster will be
#' processed as a whole. Default is `TRUE`.
#' @param spatial_fun Function used to summarise multiple values within a
#' polygon or line. Is passed to `extraction_fun` internally. Defaults to
#' `mean`.
#' @param na.rm Whether to remove NA values when summarising with the
#' `spatial_fun` function.
#' @param ... Additional arguments to pass to `terra::extract`.
#' @param extraction_fun The extraction function to use. Default is
#' `terra::extract`.
#' @param max_ram_frac_per_chunk The maximum fraction of available memory to use
#' for each extraction chunk.
#' @param resample_scale The scale to resample your raster to (in units of the
#' original raster). Leave as NULL (the default) if you do not want any
#' resampling.
#' @param resample_fun The function to use for resampling. See `method` in
#' \link[terra]{resample}.
#' @param verbose Whether to print messages to the console. Defaults to TRUE.
#'
#' @return A matrix or list where each column corresponds to a raster layer and
#' each row corresponds to a geometry in `x`. The values represent the raster
#' values at each point's location.
#'
#' @details
#' This function uses the `terra::mem_info` function to assess the RAM
#' requirements for the extraction. If chunking is required (i.e., the raster
#' data does not fit in available RAM), the raster is divided into a number of
#' chunks such that each chunk's RAM requirement does not exceed the available
#' RAM. The function then processes each chunk sequentially, extracting the
#' raster values over the spatial object, and combines the results at the end.
#' Progress of the extraction is displayed.
#'
#' @examples
#' # Assuming 'some_raster' is a terra::SpatRaster object and 'some_sp' is a
#' # spatial object:
#' # result <- extract_over_space(x=some_sp, r=some_raster)
#' @export
extract_over_space <- function(
  x,
  r,
  spatial_fun=mean,
  na.rm=TRUE,
  chunk=TRUE,
  max_ram_frac_per_chunk=1.0,
  extraction_fun=terra::extract,
  resample_scale=NULL,
  resample_fun=NULL,
  verbose=TRUE,
  ...
) {
  # drop duplicate rows that don't need to be extracted multiple times
  geometry_column_name <- attr(x, "sf_column")
  x <- x %>% dplyr::group_by(across(c(!!geometry_column_name))) %>%
    dplyr::mutate(envfetch__duplicate_spatial_ID = dplyr::cur_group_id()) %>%
    dplyr::ungroup()

  unique_x <- x[!duplicated(x$envfetch__duplicate_spatial_ID),]

  mem_info_func <- purrr::quietly(terra::mem_info)
  mem_info <- mem_info_func(r)$result
  ram_required <- mem_info['needed']
  ram_available <- mem_info['available'] * 0.9
  ram_available_per_chunk <- ram_available * max_ram_frac_per_chunk

  if (verbose) {
    cli::cli_alert(
      cli::col_black(
        paste(
          ram_required,
          'Kbs of RAM is required for extraction and',
          ram_available_per_chunk,
          'Kbs of RAM is available'
        )
      )
    )
  }

  if (requireNamespace("exactextractr", quietly = TRUE)) {
    if (identical(extraction_fun, exactextractr::exact_extract)) {
      if (any(sf::st_geometry_type(unique_x) == "POINT")) {
        stop("POINT detected in input geometry. exactextractr::exact_extract only works with polygons. Use terra::extract instead.")
      }
    }
  }

  if (chunk == TRUE && ram_required > ram_available_per_chunk) {
    # split raster into chunks based on available RAM
    times <- terra::time(r)

    num_chunks <- ceiling(ram_required / ram_available_per_chunk)
    chunk_size <- ceiling(length(times) / num_chunks)
    if (verbose)
      cli::cli_alert(cli::col_black(paste('Splitting job into', num_chunks, 'chunks')))

    if (verbose)
      pb <- cli::cli_progress_bar("Extracting data in chunks", total = num_chunks)

    # initialize list to hold chunks
    r_chunks <- vector("list", num_chunks)

    # divide raster into chunks
    for (i in seq_len(num_chunks)) {
      start_index <- ((i - 1) * chunk_size) + 1
      end_index <- min(i * chunk_size, length(times))
      r_chunks[[i]] <- r[[start_index:end_index]]
    }

    extractions <- lapply(r_chunks, function(chunk) {
      # run garbage collector to free up memory between extractions
      # gc()

      if (!is.null(resample_scale)) {
        chunk <- resample_raster(chunk, resample_scale, resample_fun)
      }

      ex <- extraction_fun(x = chunk, y = unique_x, spatial_fun=spatial_fun, na.rm=na.rm, ...)
      # update progress bar after each extraction
      if (verbose)
        cli::cli_progress_update(id=pb)
      return(ex)
    })
    # perform extraction on each chunk and combine results
    extracted <- do.call(cbind, extractions)
    # remove duplicate envfetch__duplicate_spatial_IDs
    extracted <- extracted[, !(duplicated(colnames(extracted)) & colnames(extracted)=='envfetch__duplicate_spatial_ID')]
    # remove duplicate IDs
    id_cols <- grep("^ID(\\.\\d+)?$", colnames(extracted))
    extracted <- extracted[, -id_cols[-1]]
  } else {
    if (!is.null(resample_scale)) {
      chunk <- resample_raster(chunk, resample_scale, resample_fun)
    }
    # in case of a crash before hand make sure to free up memory
    # gc()

    # perform extraction normally if raster fits in RAM
    extracted <- extraction_fun(x = r, y = unique_x, spatial_fun=spatial_fun, na.rm=na.rm, ...)
  }

  if (verbose)
    cli::cli_alert(cli::col_black('Completed extraction'))

  # a final garbage collection so later extractions have cleared up memory
  # beforehand
  # gc()

  # rename ID
  colnames(extracted)[colnames(extracted) == 'ID'] <- 'envfetch__duplicate_spatial_ID'

  # make an id column for x. this will function as the ID column that terra normally provides
  # to match multiple extracted values for polygons and lines
  x$ID <- 1:nrow(x)

  # left join the extracted data to the input
  x <- x %>% sf::st_drop_geometry() %>% dplyr::left_join(
    tibble::as_tibble(extracted),
    by = "envfetch__duplicate_spatial_ID"
  )

  cols_to_return <- colnames(extracted)[!(colnames(extracted) %in% c('envfetch__duplicate_spatial_ID', 'ID'))]

  # make sure ID is at the end of the output
  # to make indexing simpler in the fetch function
  return(x[,c(cols_to_return, 'ID')])
}

resample_raster <- function(r, res, resample_fun=NULL) {
  # Get the current resolution of the raster r
  current_res <- terra::res(r)

  cli::cli_alert(cli::col_black(paste('resampling raster from res of', current_res, 'to', res)))

  # if the current resolution is different from the desired one
  if (current_res[1] != resample_scale || current_res[2] != resample_scale) {

    # create a new empty raster with the desired resolution
    ext <- terra::ext(r)
    ncol_new <- ceiling((terra::xmax(ext) - terra::xmin(ext)) / resample_scale)
    nrow_new <- ceiling((terra::ymax(ext) - terra::ymin(ext)) / resample_scale)

    template_raster <- terra::raster(ext, ncol = ncol_new, nrow = nrow_new, crs = terra::crs(r))

    # perform the resampling
    if (is.null(resample_fun)) {
      cli::cli_alert(cli::col_black('using default resampling function'))
      r <- terra::resample(r, template_raster)
    } else {
      cli::cli_alert(cli::col_black(paste('using a', resample_fun, 'resampling function')))
      r <- terra::resample(r, template_raster, method=resample_fun)
    }
  }
  return(r)
}
