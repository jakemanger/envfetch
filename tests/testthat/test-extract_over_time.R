envfetch_vs_terra <- function(temporal_fun, polygons=FALSE) {
  d <- create_test_d(polygons=polygons)
  r <- load_test_raster()

  # extract with envfetch
  out <- d %>%
    fetch(
      ~extract_over_time(
        .x,
        r,
        temporal_fun=temporal_fun,
        spatial_fun=NULL
      ),
      .time_rep=time_rep(interval=lubridate::days(1), n_start=-2),
      use_cache=FALSE,
      out_filename=NA
    )

  # extract with terra and some mean math
  terra_out <- terra::extract(r, d, ID=TRUE)
  ID_column <- terra_out$ID
  terra_out <- terra_out[,!(colnames(terra_out) %in% 'ID')]
  times <- terra::time(r)
  time_offsets <- c(0, 1, 2)
  terra_result_matrix <- matrix(NA, nrow=nrow(d), ncol=length(time_offsets))
  for (t in time_offsets) {
    for (i in seq_len(nrow(d))) {
      time <- d$time_column[i]

      if (t > 0) {
        start <- lubridate::int_start(time)
        time <- lubridate::interval(
          start=start-lubridate::days(t),
          end=start-lubridate::days(t-1) - lubridate::minutes(1)
        )
      }

      cols_to_average <- lubridate::`%within%`(times, time)

      result <- temporal_fun(as.numeric(unlist(terra_out[ID_column == i, cols_to_average])))
      terra_result_matrix[i, t+1] <- result
    }
  }

  # compare results
  # within time period mean
  expect_equal(out$small, terra_result_matrix[,1])
  expect_equal(out$small_1_0, terra_result_matrix[,2])
  expect_equal(out$small_2_1, terra_result_matrix[,3])
}

test_that('order_of_extracted_variables_is_correct', {
  # if the order of extracted variables and how they are assigned to the data is messed up
  # then this test should fail
  r <- terra::rast(system.file('testdata', 'test_raster.tif', package='envfetch'))
  terra::time(r) <- lubridate::date(c('2010-07-01', '2010-07-02'))

  dat_with_dates <- readRDS(system.file('testdata', 'test_geometry.RDS', package='envfetch'))

  # do the extraction
  extracted <- envfetch(
    x = dat_with_dates,
    r = r,
    temporal_fun = 'mean',
    scale=NULL,
    time_column_name='dates',
    use_cache=FALSE,
    .time_rep = time_rep(interval = lubridate::days(14), n_start = -26, n_end = 0)
  )

  # define values to subset for our check
  date_ranges <- unique(extracted$dates)
  date_range <- date_ranges[1]

  extracted_1 <- extracted[as.character(extracted$dates) == as.character(date_range), ]

  times <- terra::time(r)
  r <- r[[lubridate::`%within%`(times, date_range)]]

  correct_extracted <- terra::extract(r, extracted_1)

  expect_equal(correct_extracted$tmin_40359, extracted_1$tmin)
})

test_that('correct_result_returned_points_mean', {
  expect_warning(envfetch_vs_terra(temporal_fun=mean), 'returning NA')
})

test_that('correct_result_returned_points_mean_na_rm', {
  fun <- function(x) {mean(x, na.rm=TRUE)}
  expect_warning(envfetch_vs_terra(temporal_fun=fun), 'returning NA')
})

test_that('correct_result_returned_points_sum', {
  envfetch_vs_terra(temporal_fun=sum)
})

test_that('correct_result_returned_points_sum_na_rm', {
  fun <- function(x) {sum(x, na.rm=TRUE)}
  envfetch_vs_terra(temporal_fun=fun)
})

test_that('correct_result_returned_polygons_mean', {
  expect_warning(envfetch_vs_terra(temporal_fun=mean, polygons=TRUE), 'returning NA')
})

test_that('correct_result_returned_polygons_mean_na_rm', {
  fun <- function(x) {mean(x, na.rm=TRUE)}
  expect_warning(envfetch_vs_terra(temporal_fun=fun, polygons=TRUE), 'returning NA')
})

test_that('correct_result_returned_polygons_sum', {
  envfetch_vs_terra(temporal_fun=sum, polygons=TRUE)
})

test_that('correct_result_returned_polygons_sum_na_rm', {
  fun <- function(x) {sum(x, na.rm=TRUE)}
  envfetch_vs_terra(temporal_fun=fun, polygons=TRUE)
})

test_that('chunking_doesnt_change_output', {
  d <- create_test_d()
  r <- load_test_raster()

  chunked_out <- d %>%
    envfetch(
      r,
      spatial_fun = "mean",
      chunk=TRUE,
      max_ram_frac_per_chunk=2e-07,  # chunk size to ensure good amount of chunks for test data
      .time_rep=time_rep(interval=lubridate::days(14), n_start=-2),
      use_cache=FALSE,
      out_filename=NA
    )

  out <- d %>%
    envfetch(
      r,
      spatial_fun = "mean",
      chunk=FALSE,
      .time_rep=time_rep(interval=lubridate::days(14), n_start=-2),
      use_cache=FALSE,
      out_filename=NA
    )

  expect_equal(chunked_out, out)
})


envfetch_vs_terra_over_time <- function(temporal_fun, polygons=FALSE) {
  d <- create_test_d(polygons=polygons, n=8, cellsize = 0.5)
  d <- rbind(d, d, d, d, d)

  r <- load_test_raster()

  # extract with envfetch
  out <- d %>%
    envfetch(
      r,
      temporal_fun,
      spatial_fun=NULL,
      .time_rep=time_rep(interval=lubridate::days(1), n_start=-2),
      use_cache=FALSE,
      out_filename=NA
    )

  # extract with terra and some mean math
  terra_out <- terra::extract(r, d, ID=TRUE)
  ID_column <- terra_out$ID
  terra_out <- terra_out[,!(colnames(terra_out) %in% 'ID')]
  times <- terra::time(r)
  time_offsets <- c(0, 1, 2)
  terra_result_matrix <- matrix(NA, nrow=nrow(d), ncol=length(time_offsets))
  for (t in time_offsets) {
    for (i in seq_len(nrow(d))) {
      time <- d$time_column[i]

      if (t > 0) {
        start <- lubridate::int_start(time)
        time <- lubridate::interval(
          start=start-lubridate::days(t),
          end=start-lubridate::days(t-1) - lubridate::minutes(1)
        )
      }

      cols_to_average <- lubridate::`%within%`(times, time)

      result <- temporal_fun(as.numeric(unlist(terra_out[ID_column == i, cols_to_average])))
      terra_result_matrix[i, t+1] <- result
    }
  }

  # compare results
  # within time period mean
  expect_equal(out$small, terra_result_matrix[,1])
  expect_equal(out$small_1_0, terra_result_matrix[,2])
  expect_equal(out$small_2_1, terra_result_matrix[,3])
}


# test_that('terra_and_stars_backend_match', {
#   d <- create_test_d()
#   r <- load_test_raster()
#
#   terra_out <- d %>%
#     fetch(
#       ~extract_over_time(
#         .x,
#         r,
#         spatial_fun = mean,
#         na.rm = TRUE
#       ),
#       .time_rep=time_rep(interval=lubridate::days(14), n_start=-1),
#       use_cache=FALSE,
#       out_filename=NA
#     )
#
#   stars_out <- d %>%
#     fetch(
#       ~extract_over_time(
#         .x,
#         r,
#         spatial_fun = mean,
#         extraction_fun = stars::st_extract,
#         na.rm = TRUE
#       ),
#       .time_rep=time_rep(interval=lubridate::days(14), n_start=-1),
#       use_cache=FALSE,
#       out_filename=NA
#     )
#
#   expect_equal(terra_out, stars_out)
# })
