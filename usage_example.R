devtools::load_all()
library(tidyverse)
library(terra)
library(rnaturalearth)
library(rnaturalearthdata)
library(sf)

get_prediction_surface <- function(
  prediction_surface_path='prediction_surface.tif'
) {

  if (file.exists(prediction_surface_path)) {
    r <- rast(prediction_surface_path)
    return(r)
  }

  print(
    'Creating prediction surface from clum data. This may take a while the
    first time it is run'
  )

  clum <- rast('//drive.irds.uwa.edu.au/SBS-DBPSD-001/data/geotiff_clum_50m1220m/geotiff_clum_50m1220m/clum_50m1220m.tif')
  info <- st_read("//drive.irds.uwa.edu.au/SBS-DBPSD-001/data/geotiff_clum_50m1220m/geotiff_clum_50m1220m/clum_50m1220m.tif.vat.dbf")

  agricultural_land_codes <- info |>
    filter(!AGRI_INDUS %in% c("Not agricultural industry",
                              "Horticulture",
                              "Intensive plant and animal industries")) |>
    pull(VALUE)

  att_table <- levels(clum)[[1]]
  att_table$NEW_CLASS <- 0

  att_table$NEW_CLASS[att_table$VALUE %in% agricultural_land_codes] <- 1

  # lets reclassify the ID of the raster to the NEW_CLASS variable
  reclassify.matrix <- cbind(att_table$VALUE, att_table$NEW_CLASS)
  # add in to convert ocean to uninhabitable (0)
  reclassify.matrix <- rbind(
    reclassify.matrix,
    matrix(c(NA, 0), nrow=1, ncol=2)
  )

  r <- classify(clum, reclassify.matrix, others=0)
  writeRaster(r, prediction_surface_path, overwrite=TRUE)
}

in_agricultural_land <- function(d) {
  clum <- rast('//drive.irds.uwa.edu.au/SBS-DBPSD-001/data/geotiff_clum_50m1220m/geotiff_clum_50m1220m/clum_50m1220m.tif')

  y <- st_transform(d, crs=st_crs(clum))
  agricultural_land <- exactextractr::exact_extract(x=clum, y=y)

  info <- st_read("//drive.irds.uwa.edu.au/SBS-DBPSD-001/data/geotiff_clum_50m1220m/geotiff_clum_50m1220m/clum_50m1220m.tif.vat.dbf")

  agricultural_land_codes <- info |>
    filter(!AGRI_INDUS %in% c("Not agricultural industry",
                              "Horticulture",
                              "Intensive plant and animal industries")) |>
    pull(VALUE)


  in_agri_land <- rep(FALSE, length(agricultural_land))
  for (i in 1:length(agricultural_land)) {
    codes_in_area <- agricultural_land[[i]]
    if (any(agricultural_land_codes %in% as.vector(codes_in_area$value))) {
      in_agri_land[[i]] <- TRUE
    }
  }

  return(in_agri_land)
}

# load map of australia for reference
world <- ne_countries(scale = "medium", returnclass = "sf")
australia <- subset(world, admin == "Australia")

prediction_surface_locations_path <- 'prediction_surface_locations_1_deg.rds'
if (file.exists(prediction_surface_locations_path)) {
  d <- readRDS(prediction_surface_locations_path)
} else {
  d <- sf::read_sf(
      "//drive.irds.uwa.edu.au/SBS-DBPSD-001/db_data_with_duplicates.csv",
      options=c('X_POSSIBLE_NAMES=X', 'Y_POSSIBLE_NAMES=Y'),
      crs=sf::st_crs(4326)
    )

  # raster_approach <- get_prediction_surface()

  # VECTOR approach
  d <- throw(
    offset=c(110, -45),
    cellsize=1,
    n=c(100, 70),
    time_interval=lubridate::date('2020-01-01'),
    what='polygons'
  )

  # remove points outside of Australian borders
  logi_d_in_pol <- st_intersects(australia, d, sparse = FALSE)
  d <- d[as.vector(logi_d_in_pol), ]

  # remove points outside those with agricultural land
  in_agri_land <- d %>% in_agricultural_land()
  d <- d[in_agri_land, ]

  saveRDS(
    d,
    prediction_surface_locations_path,
  )
}

# plot
ggplot(data = australia) +
  geom_sf() +
  geom_sf(data = d, size = 1, show.legend = "point") +
  ggtitle("Australia with sample points") +
  theme_minimal()


days <- 365
orig_d <- d

for (i in 1:days) {
  new_d_rows <- orig_d
  new_d_rows$time_column <- lubridate::as_date(new_d_rows$time_column) + days(1)
  d <- bind_rows(
    d,
    new_d_rows
  )
}

# get the centroids
centroids <- st_centroid(d)

extracted_dbee_1 <-
  centroids[1:50,] |>
  fetch(
    ~extract_over_time(.x, r = rast("//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP from 1950.nc", subds='precip'), temporal_fun=sum),
    # ~extract_over_time(.x, r = rast("//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP from 1950.nc", subds=c('tmin', 'tmax', 'vprp'))),
    # ~extract_over_time(.x, r = "//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP solar from 1990.nc"),
    # ~extract_over_time(
    #   .x,
    #   r = list.files(
    #     "//drive.irds.uwa.edu.au/SBS-DBPSD-001/Manually_downloaded_data/Australian_water_outlook/Root_zone_soil_moisture",
    #     pattern = "\\.nc$",
    #     full.names = TRUE
    #   )
    # )
    # ~extract_gee(
    #   .x,
    #   collection_name='MODIS/061/MOD13Q1',
    #   bands=c('NDVI', 'DetailedQA'),
    #   time_buffer= lubridate::days(16)
    # ),
    .time_rep = time_rep(interval = lubridate::days(14), n_start = -1, n_end = 0)
  )

result <- microbenchmark::microbenchmark(
  parallel = {
  extracted_dbee_1 <-
    centroids[1:5000,] |>
    fetch(
      ~extract_over_time(.x, r = rast("//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP from 1950.nc", subds='precip'), temporal_fun=sum),
      .time_rep = time_rep(interval = lubridate::days(14), n_start = -1, n_end = 0),
      use_cache = FALSE
    )
  },
  parallel_4_cores = {
    extracted_dbee_1 <-
      centroids[1:5000,] |>
      fetch(
        ~extract_over_time(.x, r = rast("//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP from 1950.nc", subds='precip'), temporal_fun=sum, workers=4),
        .time_rep = time_rep(interval = lubridate::days(14), n_start = -1, n_end = 0),
        use_cache = FALSE
      )
  },
  non_parallel = {
    extracted_dbee_1 <-
      centroids[1:5000,] |>
      fetch(
        ~extract_over_time(.x, r = rast("//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP from 1950.nc", subds='precip'), temporal_fun=sum, parallel=FALSE),
        .time_rep = time_rep(interval = lubridate::days(14), n_start = -1, n_end = 0),
        use_cache = FALSE
      )
  },
  times=2,
  check='equal'
)

# time_reps <- time_rep(interval = lubridate::days(1), n_start = -365, n_end = 0)
#
# extracted_dbee_1 <- centroids %>%
#   envfetch(
#     r = "//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP from 1950.nc",
#     bands = 'precip',
#     temporal_fun = sum,
#     .time_rep = time_reps
#   ) %>%
#   envfetch(
#     r = "//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP from 1950.nc",
#     bands = c('tmin', 'tmax', 'vprp'),
#     temporal_fun = mean,
#     .time_rep = time_reps
#   ) %>%
#   envfetch(
#     r = "//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP solar from 1990.nc",
#     temporal_fun = sum,
#     .time_rep = time_reps
#   ) %>%
#   envfetch(
#     r = list.files(
#       "//drive.irds.uwa.edu.au/SBS-DBPSD-001/Manually_downloaded_data/Australian_water_outlook/Root_zone_soil_moisture",
#       pattern = "\\.nc$",
#       full.names = TRUE
#     ),
#     temporal_fun = mean,
#     .time_rep = time_reps
#   ) %>%
#   envfetch(
#     r = 'MODIS/061/MOD13Q1',
#     bands = c('NDVI', 'DetailedQA'),
#     temporal_fun = mean,
#     .time_rep = time_reps
#   )


write.csv(extracted_dbee_1, "data/extracted_dbee.csv", row.names = FALSE)
