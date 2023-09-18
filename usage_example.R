devtools::load_all()
library(tidyverse)
library(terra)
library(rnaturalearth)
library(rnaturalearthdata)
library(sf)

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
  new_d_rows$time_column <- lubridate::as_date(new_d_rows$time_column) + days(i)
  d <- bind_rows(
    d,
    new_d_rows
  )
}

# get the centroids
centroids <- d
st_geometry(centroids) <- st_centroid(st_geometry(d))

# do the extraction
extracted <- envfetch(
  x = centroids,
  r = list(
    # "//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP from 1950.nc",
    # "//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP from 1950.nc",
    # "//drive.irds.uwa.edu.au/SBS-DBPSD-001/AWAP-Climate-Data/data/AWAP solar from 1990.nc",
    # list.files(
    #   "//drive.irds.uwa.edu.au/SBS-DBPSD-001/Manually_downloaded_data/Australian_water_outlook/Root_zone_soil_moisture",
    #   pattern = "\\.nc$",
    #   full.names = TRUE
    # ),
    'MODIS/061/MOD13Q1'
  ),
  bands = list(
    # 'precip',
    # c('tmin', 'tmax', 'vprp'),
    # NULL,
    # NULL,
    c('NDVI', 'DetailedQA')
  ),
  temporal_fun = list(
    # 'sum',
    # 'mean',
    # 'mean',
    # 'mean',
    'last'
  ),
  .time_rep = time_rep(interval = lubridate::days(14), n_start = -26, n_end = 0),
  use_drive=TRUE
)

saveRDS(extracted, 'prediction_surface.RDS')

# plot to look at result
filtered_data <- extracted %>%
  filter(envfetch__original_time_column == extracted$envfetch__original_time_column[1])

ggplot(filtered_data) +
  geom_sf(aes(color = precip)) +  # Color by 'precip' variable
  scale_color_viridis_c() +  # Choose a color scale
  theme_minimal() +
  ggtitle("Plot of Selected Time Slice with Colors for 'precip'")

