test_that('extraction_works_with_intervals', {
  d <- create_test_d()
  r <- load_test_raster()

  expect_no_error(
    { res <- d %>% envfetch(r=list(r, r), use_cache = FALSE) }
  )
})

test_that('extraction_works_with_dates', {
  d <- create_test_d()
  r <- load_test_raster()
  d$time_column <- lubridate::int_start(d$time_column)
  expect_no_error(
    { res <- d %>% envfetch(r=r, use_cache = FALSE) }
  )
})

test_that('extraction_works_with_dates_as_strings', {
  d <- create_test_d()
  r <- load_test_raster()
  d$time_column <- as.character(lubridate::int_start(d$time_column))
  expect_no_error(
    { res <- d %>% envfetch(r=r, use_cache = FALSE) }
  )
})

test_that('extraction_works_with_datetimes', {
  d <- create_test_d()
  r <- load_test_raster()
  d$time_column <- lubridate::int_start(d$time_column) + lubridate::seconds(10)
  expect_no_error(
    { res <- d %>% envfetch(r=r, use_cache = FALSE) }
  )
})

test_that('extra_cols_are_appended_back', {
  d <- create_test_d()
  d$extra_col_1 <- paste0('extra_col_info_', 1:nrow(d))
  d$extra_col_2 <- paste0('second_extra_col_info_', 1:nrow(d))
  r <- load_test_raster()
  expect_no_error(
    { res <- d %>% envfetch(r=r, use_cache = FALSE) }
  )

  expect_equal(d$extra_col_1, res$extra_col_1)
  expect_equal(d$extra_col_2, res$extra_col_2)
  # only extra columns should be 'small' in this case
  original_column_order <- colnames(d)
  current_column_order <- colnames(res)
  new_colnames <- current_column_order[!(current_column_order %in% original_column_order)]
  expect_equal(new_colnames, c('small'))
})

test_that('caches_between_runs', {
  d <- create_test_d()
  r <- load_test_raster()

  cache_dir <- paste0(tempdir(), '/cache_dir_1/')

  if (dir.exists(cache_dir)) unlink(cache_dir, recursive=TRUE)

  expect_message(first_out <- d %>% envfetch(r=r, cache_dir=cache_dir, verbose=TRUE), 'Completed')
  expect_message(second_out <- d %>% envfetch(r=r, cache_dir=cache_dir, verbose=TRUE), 'Dug up cached result')

  expect_equal(first_out, second_out) # should return the same result
})

test_that('caching_saves_time_within_runs', {
  d <- create_test_d()
  r <- load_test_raster()

  cache_dir <- paste0(tempdir(), '/cache_dir_2/')

  if (dir.exists(cache_dir)) unlink(cache_dir, recursive=TRUE)

  t1 <- Sys.time()
  without_cache <- d %>% envfetch(r=list(r, r), use_cache = FALSE)
  t2 <- Sys.time()
  with_cache <- d %>% envfetch(r=list(r, r), cache_dir=cache_dir)
  t3 <- Sys.time()

  expect_lt(as.numeric(t3-t2), as.numeric(t2-t1))

  expect_equal(with_cache, without_cache) # should return the same result
})

test_that('caching_does_not_cache_different_rasters', {
  d <- create_test_d()
  r <- load_test_raster()

  cache_dir <- paste0(tempdir(), '/cache_dir_3/')

  if (dir.exists(cache_dir)) unlink(cache_dir, recursive=TRUE)
  r2 <- r + 10

  t1 <- Sys.time()
  first_out <- d %>% envfetch(r=r, cache_dir=cache_dir)
  t2 <- Sys.time()
  second_out <- d %>% envfetch(r=r2, cache_dir=cache_dir)
  t3 <- Sys.time()

  first_out$small <- first_out$small + 10 # second_out should be 10 larger
  expect_equal(first_out, second_out) # should return the same result
})
