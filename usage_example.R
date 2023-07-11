load_all()
library(tidyverse)
# d <- throw(
#   offset=c(115, -40),
#   cellsize=3,
#   n=20,
#   time_interval=lubridate::interval(start='2017-01-01', end='2017-01-02'),
# )

# d$new_column = 'banana'

d <- sf::read_sf(
    r"(C:\Users\00099357\projects\envfetch\output\extracted_wa_test.csv)",
    options=c('X_POSSIBLE_NAMES=X', 'Y_POSSIBLE_NAMES=Y'),
    crs=sf::st_crs(4326)
  ) %>%
  dplyr::mutate(
    time_column =
      lubridate::interval(
        stringr::str_replace_all(eventDate, "\\+0800", ":00\\+0800"),
        tzone = "Australia/Perth"),
    .after = eventDate
  )

extracted <- d[1:10,] %>%
  fetch(
    ~extract_across_times(
      .x,
      "//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP from 1950.nc",
    ),
    ~extract_gee(
       .x,
       collection_name='MODIS/061/MOD13Q1',
       bands=c('NDVI', 'DetailedQA'),
       use_drive=TRUE
    ),
    .time_rep=time_rep(interval=lubridate::days(14), n_start=-2),
    use_cache=TRUE
  )
