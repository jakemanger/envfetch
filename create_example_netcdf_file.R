library(terra)

# The original data is freely downloadable in yearly netcdf format via:
# https://awo.bom.gov.au/products/historical/soilMoisture-rootZone/4.5,-27.509,134.221/nat,-25.609,134.362/r/d/2023-07-16
# and is licensed under CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/).

# generates testing data for the readme example
path_to_ncs <- "//drive.irds.uwa.edu.au/SBS-DBPSD-001/Manually_downloaded_data/Australian_water_outlook/Root_zone_soil_moisture/"
nc_paths <- list.files(path_to_ncs, full.names=TRUE)
r <- rast(nc_paths)

# Subset by time (e.g., take first 5 layers)
r <- r[[time(r) >= '2017-01-01' & time(r) <= '2017-01-02']]

# Subset by spatial extent
r_subset <- crop(r, ext(116, 144, -39, -11))

# plot to check the data
plot(r_subset)

writeCDF(r_subset, "./inst/extdata/example.nc", overwrite=TRUE)

# test reading
# r <- rast('./inst/extdata/example.nc')
#
# plot(r)
