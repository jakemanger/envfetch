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

test_that('correct_result_returned_points_mean', {
  envfetch_vs_terra(temporal_fun=mean)
})

test_that('correct_result_returned_points_mean_na_rm', {
  fun <- function(x) {mean(x, na.rm=TRUE)}
  envfetch_vs_terra(temporal_fun=fun)
})

test_that('correct_result_returned_points_sum', {
  envfetch_vs_terra(temporal_fun=sum)
})

test_that('correct_result_returned_points_sum_na_rm', {
  fun <- function(x) {sum(x, na.rm=TRUE)}
  envfetch_vs_terra(temporal_fun=fun)
})

test_that('correct_result_returned_polygons_mean', {
  envfetch_vs_terra(temporal_fun=mean, polygons=TRUE)
})

test_that('correct_result_returned_polygons_mean_na_rm', {
  fun <- function(x) {mean(x, na.rm=TRUE)}
  envfetch_vs_terra(temporal_fun=fun, polygons=TRUE)
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
    fetch(
      ~extract_over_time(
        .x,
        r,
        spatial_fun = mean,
        na.rm = TRUE,
        chunk = TRUE
      ),
      .time_rep=time_rep(interval=lubridate::days(14), n_start=-1),
      use_cache=FALSE,
      out_filename=NA
    )

  out <- d %>%
    fetch(
      ~extract_over_time(
        .x,
        r,
        fun = mean,
        na.rm = TRUE,
        chunk = FALSE
      ),
      .time_rep=time_rep(interval=lubridate::days(14), n_start=-1),
      use_cache=FALSE,
      out_filename=NA
    )

  expect_equal(chunked_out, out)
})
