
<!-- README.md is generated from README.Rmd. Please edit that file -->

# envfetch <a href="https://github.com/jakemanger/envfetch"><img src="man/figures/logo.png" align="right" height="238"/></a>

<!-- badges: start -->
<!-- badges: end -->

Your trusty companion to fetch, extract and summarise environmental data
across time and space using gps coordinates or `sf` polygons.

## Installation

You can install the development version of envfetch like so:

``` r
remotes::install_github('jakemanger/envfetch')
```

## Example

Use of envfetch starts with a table: a `dataframe`, `tibble` or `sf`
object.

Lets generate one for testing by sampling a grid over Australia for a
range of times using the `throw` function:

``` r
# library(envfetch)

# d <- throw(
#   aoi=ext(c(110, 170, -70, 10)),
#   time_interval=lubridate::interval()
#   type='grid'
# )
```

The data set should look like the following:

``` r
# summary(d)
```

Note, each data point has its own `sf::geometry` object along with its
own `datetime` (a `lubridate::interval`).

This `geometry` may be a point or a polygon. You may also just use plain
old `x` and `y` coordinates as separate columns.

Each individual data point will use the `datetime` object to decide what
time range to extract and summarise data in. `datetime` can be a time
range (`lubridate::interval`), a single date (e.g. `"20220101"`) or
datetime `"2010-08-03 00:50:50"`.

In this example, let’s assume we have:

-   some pre-downloaded netcdf file we would like to extract from (CLUM)

-   and a data set from Google Earth Engine we would like to download
    and extract (NDVI from MODIS)

We also want to:

-   calculate day and night time statistics for each data point and
    time.

To fetch the data, use the `fetch` function and supply it with the
extraction functions you would like to use, e.g.:

``` r
# extracted <- d %>%
#   fetch(
#    
#   )
# TODO
```

Now, certain applications might require obtaining the same data at
repeated time intervals. For instance, rather than acquiring data solely
for the data point’s `datetime` time range, you may also need
environmental data from repeated previous time periods. In this example,
we will use the `.time_rep` variable to extract some of our
environmental data from the past six months, with an average calculated
for each two week block.

``` r
# rep_extracted <- d %>%
#   fetch(
#     
#     .time_rep=time_steps(n_step=-13, every=lubridate::days(14))
#   )
# TODO
```
