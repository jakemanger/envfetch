test_that('caching_saves_time_between_runs', {
  d <- create_test_d()
  r <- load_test_raster()

  cache_dir <- paste0(tempdir(), '/cache_dir_1/')

  if (dir.exists(cache_dir)) unlink(cache_dir, recursive=TRUE)

  t1 <- Sys.time()
  first_out <- d %>% envfetch(r=r, cache_dir=cache_dir)
  t2 <- Sys.time()
  second_out <- d %>% envfetch(r=r, cache_dir=cache_dir)
  t3 <- Sys.time()

  expect_lt(as.numeric(t3-t2), as.numeric(t2-t1)*0.5) # should be at least 50% faster

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

  expect_lt(as.numeric(t3-t2), as.numeric(t2-t1)*0.7) # should be at least 30% faster

  expect_equal(with_cache, without_cache) # should return the same result
})
