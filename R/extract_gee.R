library(rgee)
ee_Initialize(gcs = TRUE, drive = TRUE)

#' Extract from google earth engine
#'
#' Use google earth engine to extract your chosen image collection bands
#' and then summarise this information for each row in your dataset (`points`)
#'
#' @param points
#' @param collection_name
#' @param bands
#' @param savepath
#' @param scale
#' @param time_buffer
#' @param time_summarise_fun
#'
#' @return
#' @export
#'
#' @examples
extract_gee <- function(points, collection_name, bands, savepath, scale=250, time_buffer=16, time_summarise_fun='last') {
  print('Loading sf object on earth engine...')
  pts <- st_geometry(points)[!duplicated(st_geometry(points))]

  p <- rgee::sf_as_ee(pts)

  # get min and max dates from the points tibble
  min_date <- as.character(as.Date(min(lubridate::int_start(points$time_column))) - time_buffer)
  max_date <- as.character(as.Date(max(lubridate::int_end(points$time_column))) + time_buffer)

  print('Loading image collection objects on earth engine...')
  print(paste('Loading object:', collection_name, 'with bands', paste(bands, collapse = ', ')))

  ic <- rgee::ee$ImageCollection(collection_name)$
    filterDate(min_date, max_date)$
    select(bands)

  print('extracting...')
  temp <- ee_extract(
    x = ic,
    y = pts,
    scale = scale,
    fun = ee$Reducer$mean(),
    via = "drive",
    lazy = FALSE,
    sf = TRUE
  )

  print('Summarising extracted data over specified times')

  geometry <- temp %>% st_geometry() %>% st_coordinates()
  temp_for_indxing <- temp[,str_starts(colnames(temp), 'X')] %>% st_drop_geometry()
  clnames <- colnames(temp_for_indxing)
  tms <- as.Date(unlist(lapply(clnames, get_date_from_gee_colname)))
  nms <- str_split_i(clnames, '_', 4)

  new_col_names <- unique(nms)

  points[, new_col_names] <- NA
  points_geom <- st_geometry(points) %>% st_coordinates()

  with_progress({
    p <- progressor(steps = nrow(points))

    for (i in 1:nrow(points)) {
      mn = lubridate::int_start(points$time_column[i])
      mx = lubridate::int_end(points$time_column[i])

      if (time_summarise_fun == 'last') {
        x_match <- geometry[,1] == points_geom[i,1]
        y_match <- geometry[,2] == points_geom[i,2]
      }

      for (col_name in new_col_names) {

        if (time_summarise_fun == 'mean') {
          col_names_to_summarise <-
            tms >= mn & tms <= mx & str_starts(nms, col_name)
          cols_to_summarise <-
            colnames(temp_for_indxing) %in% nms[col_names_to_summarise]
          points[i, col_name] <-
            mean(as.numeric(temp_for_indxing[i, cols_to_summarise]), na.rm = TRUE)

        } else if (time_summarise_fun == 'last') {
          col_names_to_summarise <- tms == tms[find_closest_date(tms, mn, find_closest_previous=TRUE)] & nms == col_name
          value <- temp_for_indxing[x_match & y_match, col_names_to_summarise]
          points[i, col_name] <- value
        }

      }
      p()
    }

  })

  saveRDS(points, savepath)

  return(points)
}

find_closest_date <- function(dates, x, find_closest_previous=TRUE) {
  dates <- as.Date(dates)
  x <- as.Date(x)
  if (find_closest_previous) {
    # add filter to only include dates earlier than x
    return(which(abs(dates[dates < x]-x) == min(abs(dates[dates < x] - x))))
  } else {
    return(which(abs(dates-x) == min(abs(dates - x))))
  }
}


get_date_from_gee_colname <- function(my_string) {
  if (!str_starts(my_string, 'X'))
    return(NA)

  split_string <- strsplit(my_string, "_")[[1]]
  date <- paste(split_string[1:3], collapse = "-")
  date <- str_remove(date, 'X')
  return(date)
}
