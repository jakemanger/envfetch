context('extract_across_times')

test_that('chunking_doesnt_change_output', {
  d <- throw(
    offset=c(115, -40),
    cellsize=3,
    n=5,
    time_interval=lubridate::interval(start='2017-01-01', end='2017-01-02'),
  )

  chunked_out <- d %>%
    fetch(
      ~extract_across_times(
        .x,
        system.file('testdata', 'test_awap.nc', package='envfetch'),
        chunk=TRUE
      ),
      .time_rep=time_rep(interval=lubridate::days(14), n_start=-1),
      use_cache=FALSE
    )

  out <- d %>%
    fetch(
      ~extract_across_times(
        .x,
        system.file('testdata', 'test_awap.nc', package='envfetch'),
        chunk=FALSE
      ),
      .time_rep=time_rep(interval=lubridate::days(14), n_start=-1),
      use_cache=FALSE
    )

  expect_equal(chunked_out, out)
})
