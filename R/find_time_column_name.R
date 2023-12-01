find_time_column_name <- function(d, names_to_ignore=c('envfetch__original_time_column', 'envfetch__duplicate_ID', 'geometry', 'row_num', 'original_order')) {
  colnms <- colnames(d)
  is_interval <- sapply(d, function(x) { lubridate::is.interval(x) | is_date(x) })
  if (!any(is_interval))
    stop('No time interval or date columns found. This function requires a time column of type lubridate::interval or lubridate::date to be used.')
  time_column_name <- colnms[is_interval & !(colnms %in% names_to_ignore)]
  if (length(time_column_name) > 1) {
    column_name_options <- paste(time_column_name, collapse=', ')
    stop(paste('More than one columns with type lubridate::interval or lubridate::Date were found. Please specify one of: ', column_name_options))
  }
  return(time_column_name)
}
