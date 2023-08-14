find_time_column_name <- function(d, names_to_ignore=c('envfetch__original_time_column')) {
  colnms <- colnames(d)
  is_interval <- sapply(d, lubridate::is.interval)
  if (!any(is_interval))
    stop('No time interval columns found. This function requires a time column of type lubridate::interval to be used.')
  time_column_name <- colnms[is_interval & !(colnms %in% names_to_ignore)]
  if (length(time_column_name) > 1) {
    column_name_options <- paste(time_column_name, sep=', ')
    stop(paste('More than one columns with type lubridate::interval were found. Please specify one of:', column_name_options))
  }
  return(time_column_name)
}
