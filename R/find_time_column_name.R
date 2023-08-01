find_time_column_name <- function(d) {
  colnms <- colnames(points)
  is_interval <- lubridate::is.interval(colnms)
  if (!any(is_interval))
    stop('No time interval columns found. This function requires a time column of type lubridate::interval to be used.')
  time_column_name <- colnms[is_interval]
  if (length(time_column_name) > 1) {
    column_name_options <- paste(time_column_name, sep=', ')
    stop(paste('More than one columns with type lubridate::interval were found. Please specify one of:', column_name_options))
  }
  return(time_column_name)
}
