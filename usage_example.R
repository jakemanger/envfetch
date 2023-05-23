load_all()

d <- throw(
  offset=c(115, -40),
  cellsize=3,
  n=10,
  time_interval=lubridate::interval(start='2017-01-01', end='2017-01-02'),
)

rep_extracted <- d %>%
  fetch(
    ~extract_across_times(
      .x,
      "//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP from 1950.nc",
    ),
    ~extract_gee(
       .x,
       collection_name='MODIS/061/MOD13Q1',
       bands=c('NDVI', 'DetailedQA'),
       time_buffer=16,
     ),
    use_cache=FALSE
   )
