create_test_d <- function() {
  d <- throw(
    offset=c(119.625, -30.775),
    cellsize=1,
    n=4,
    time_interval=lubridate::interval(start='2018-01-01', end='2018-01-04'),
  )
  d$time_column[1:2] <- lubridate::interval(start='2018-01-02', end='2018-01-04')
  return(d)
}

envfetch_vs_terra <- function(temporal_fun) {
  d <- create_test_d()
  r <- terra::rast(system.file('testdata', 'test_tmin.nc', package='envfetch'))

  # extract with envfetch
  out <- d %>%
    fetch(
      ~extract_over_time(
        .x,
        r,
        temporal_fun=temporal_fun
      ),
      .time_rep=time_rep(interval=lubridate::days(1), n_start=-2),
      use_cache=FALSE,
      out_filename=NA
    )

  # extract with terra and some mean math
  terra_out <- terra::extract(r, d, ID=FALSE)
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
          end=start-lubridate::days(t-1)
        )
      }

      cols_to_average <- lubridate::`%within%`(times, time)

      result <- temporal_fun(as.numeric(terra_out[i, cols_to_average]))
      terra_result_matrix[i, t+1] <- result
    }
  }

  # compare results
  # within time period mean
  expect_equal(out$small, terra_result_matrix[,1])
  expect_equal(out$small_1_0, terra_result_matrix[,2])
  expect_equal(out$small_2_1, terra_result_matrix[,3])
}

test_that('correct_result_returned_mean', {
  envfetch_vs_terra(temporal_fun=mean)
})

test_that('correct_result_returned_mean_na_rm', {
  fun <- function(x) {mean(x, na.rm=TRUE)}
  envfetch_vs_terra(temporal_fun=fun)
})

test_that('correct_result_returned_sum', {
  envfetch_vs_terra(temporal_fun=sum)
})

test_that('correct_result_returned_sum_na_rm', {
  fun <- function(x) {sum(x, na.rm=TRUE)}
  envfetch_vs_terra(temporal_fun=fun)
})

test_that('chunking_doesnt_change_output', {
  d <- create_test_d()

  chunked_out <- d %>%
    fetch(
      ~extract_over_time(
        .x,
        system.file('testdata', 'test_tmin.nc', package='envfetch'),
        spatial_extraction_fun=function(x, r) {
          extract_over_space(
            x = x,
            r = r,
            fun = mean,
            na.rm = TRUE,
            chunk = TRUE
          )
        }
      ),
      .time_rep=time_rep(interval=lubridate::days(14), n_start=-1),
      use_cache=FALSE,
      out_filename=NA
    )

  out <- d %>%
    fetch(
      ~extract_over_time(
        .x,
        system.file('testdata', 'test_tmin.nc', package='envfetch'),
        spatial_extraction_fun=function(x, r) {
          extract_over_space(
            x = x,
            r = r,
            fun = mean,
            na.rm = TRUE,
            chunk = FALSE
          )
        }
      ),
      .time_rep=time_rep(interval=lubridate::days(14), n_start=-1),
      use_cache=FALSE,
      out_filename=NA
    )

  expect_equal(chunked_out, out)
})
