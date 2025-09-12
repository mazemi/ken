#-----------------------------------------------------------------------------
# detect.similarity()
# Detects similar rows within a dataframe based on specified columns.
# Calculates similarity percentage and difference for each pair of rows.
# Returns a list with detailed similarity and matched dataframe for analysis.
#-----------------------------------------------------------------------------
detect.similarity <- function(df, based_on, uuid_col = "_uuid",
                                  max_difference = 20, extra_cols = NULL) {
  
  # Sort df if possible
  tryCatch({
    if ("start" %in% names(df)) {
      df <- df %>% arrange(start)
    } else if ("_index" %in% names(df)) {
      df <- df %>% arrange(`_index`)
    }
  }, error = function(e) {
    warning("Could not sort data by 'start' or '_index'. Proceeding unsorted.")
  })
  
  cols_to_ignore <- c(
    "start", "end", "deviceid", "audit", "audit_URL", "_id", "_uuid",
    "_submission_time", "_validation_status", "_notes", "_status",
    "_submitted_by", "__version__", "_tags", "_index",
    "interview_duration_minutes", "delayed_time_minutes", "form_exited",
    "number_of_navigation", "edit_count", "audit_remarks",
    "audit_gps_maximum_movement", "audit_centroid_latitude",
    "audit_centroid_longitude", "audit_gps_min_accuracy",
    "audit_gps_max_accuracy", "audit_no_gps", "enum_distance", "duration"
  )
  
  cols_to_check <- setdiff(names(df), c(based_on, uuid_col, cols_to_ignore))
  
  # Convert all comparison columns to character and replace NA with "NA"
  df[cols_to_check] <- lapply(df[cols_to_check], function(x) {
    x <- as.character(x)
    x[is.na(x)] <- "NA"
    x
  })
  
  groups <- df %>% group_by(across(all_of(based_on))) %>% group_split()
  group_keys <- df %>% group_by(across(all_of(based_on))) %>% group_keys()
  n_groups <- length(groups)
  n_bar <- 70
  
  results <- lapply(seq_along(groups), function(idx) {
    data_enum <- groups[[idx]]
    key_vals <- group_keys[idx, , drop = FALSE]
    n <- nrow(data_enum)
    
    if (n < 2) {
      progress <- idx / n_groups
      n_dashes <- round(progress * n_bar)
      bar <- paste0("|", strrep("-", n_dashes), strrep(" ", n_bar - n_dashes), "|")
      cat(bar, "\r")
      return(NULL)
    }
    
    mat <- as.matrix(data_enum[, cols_to_check, drop = FALSE])
    combs <- utils::combn(n, 2)
    out_list <- vector("list", ncol(combs))
    
    for (i in seq_len(ncol(combs))) {
      r1 <- combs[1, i]; r2 <- combs[2, i]
      v1 <- mat[r1, ]; v2 <- mat[r2, ]
      
      # Count matches and mismatches directly
      comparison <- v1 == v2
      matches <- sum(comparison, na.rm = TRUE)
      mismatches <- length(comparison) - matches
      
      difference <- mismatches
      total_columns <- length(cols_to_check)
      
      if (difference <= max_difference) {
        similarity_percentage <- (matches / total_columns) * 100
        
        res <- tibble(
          uuid1 = data_enum[[uuid_col]][r1],
          uuid2 = data_enum[[uuid_col]][r2],
          similarity_percentage = round(similarity_percentage, 1),
          difference = difference
        )
        res <- bind_cols(key_vals, res)
        
        if (!is.null(extra_cols)) {
          for (col in extra_cols) {
            if (col %in% names(data_enum)) {
              res[[paste0(col, "_1")]] <- data_enum[[col]][r1]
              res[[paste0(col, "_2")]] <- data_enum[[col]][r2]
            }
          }
        }
        out_list[[i]] <- res
      }
    }
    
    progress <- idx / n_groups
    n_dashes <- round(progress * n_bar)
    bar <- paste0("|", strrep("-", n_dashes), strrep(" ", n_bar - n_dashes), "|")
    cat(bar, "\r")
    
    if (length(out_list) > 0) bind_rows(out_list) else NULL
  })
  
  cat("\nSimilarity calculation finished.\n")
  
  similarity_percentage <- bind_rows(results)
  
  if (nrow(similarity_percentage) > 0) {
    similarity_percentage <- similarity_percentage %>%
      arrange(difference, desc(similarity_percentage))
    
    df_long <- similarity_percentage %>%
      select(uuid1, uuid2, similarity_percentage) %>%
      pivot_longer(cols = c(uuid1, uuid2),
                   names_to = "pair_role",
                   values_to = uuid_col)
    
    similar_data <- df %>%
      inner_join(df_long, by = uuid_col) %>%
      relocate(similarity_percentage, .after = last_col()) %>%
      arrange(desc(similarity_percentage))
  } else {
    similarity_percentage <- tibble()
    similar_data <- tibble()
  }
  
  return(list(
    similarity_percentage = similarity_percentage,
    similar_data = similar_data
  ))
}


#-----------------------------------------------------------------------------
# detect.overlaps()
# Finds overlapping time intervals for rows grouped by enumerator.
# Converts start and end times safely and checks for valid ranges.
# Returns a dataframe of UUID pairs with overlapping periods.
#-----------------------------------------------------------------------------
detect.overlaps <- function(df, start_col = "start", end_col = "end", uuid_col = "_uuid", enumerator_col) {
  
  # --- Validation checks ---
  required_cols <- c(start_col, end_col, uuid_col, enumerator_col)
  missing_cols <- setdiff(required_cols, names(df))
  if (length(missing_cols) > 0) {
    stop(paste("Missing required column(s):", paste(missing_cols, collapse = ", ")))
  }
  
  if (!is.data.frame(df)) stop("Input must be a data frame.")
  if (nrow(df) == 0) return(data.frame()) # return empty if no rows
  
  auto_datetime <- function(x) {
    if (is.numeric(x)) {
      as.POSIXct((x - 25569) * 86400, origin = "1970-01-01", tz = "UTC")
    } else {
      suppressWarnings(
        lubridate::parse_date_time(
          x,
          orders = c("ymd HMS", "ymd HM", "ymd",
                     "dmy HMS", "dmy HM", "dmy",
                     "mdy HMS", "mdy HM", "mdy",
                     "ymdTz", "ymdT", "ymd_HMS") 
        )
      )
    }
  }
  

  df <- df %>%
    mutate(
      start_dt = auto_datetime(.data[[start_col]]),
      end_dt   = auto_datetime(.data[[end_col]])
    )
  
  df <- df %>% filter(!is.na(start_dt) & !is.na(end_dt))
  
  if (nrow(df) == 0) {
    warning("No valid datetime rows after parsing.")
    return(data.frame())
  }
  
  # Check start < end
  invalid_ranges <- sum(df$start_dt >= df$end_dt, na.rm = TRUE)
  if (invalid_ranges > 0) {
    warning(paste(invalid_ranges, "rows have start >= end, excluded."))
    df <- df %>% filter(start_dt < end_dt)
  }
  
  # Overlap check helper 
  ranges_overlap <- function(a_start, a_end, b_start, b_end) {
    (a_start < b_end) & (b_start < a_end)
  }
  
  overlap_results <- df %>%
    group_by(.data[[enumerator_col]]) %>%
    do({
      n <- nrow(.)
      res <- data.frame()
      if (n > 1) {
        for (i in 1:(n-1)) {
          for (j in (i+1):n) {
            if (ranges_overlap(.$start_dt[i], .$end_dt[i], .$start_dt[j], .$end_dt[j])) {
              res <- rbind(res, data.frame(
                uuid1 = .[[uuid_col]][i],
                uuid2 = .[[uuid_col]][j],
                range_datetime1 = paste0(format(.$start_dt[i], "%Y-%m-%d %H:%M"), " - ",
                                         format(.$end_dt[i], "%Y-%m-%d %H:%M")),
                range_datetime2 = paste0(format(.$start_dt[j], "%Y-%m-%d %H:%M"), " - ",
                                         format(.$end_dt[j], "%Y-%m-%d %H:%M")),
                
                col_start = start_col
              ))
            }
          }
        }
      }
      res
    }) %>% ungroup()
  cat("\nOverlap process finished.\n")
  return(overlap_results)
}

#-----------------------------------------------------------------------------
# add.duration()
# Adds a duration column to a dataframe by calculating the difference 
# between start and end times in minutes.
# Supports multiple time formats including POSIXct, character, and numeric.
#-----------------------------------------------------------------------------
add.duration <- function(df, start_col = "start", end_col = "end", 
                         output_col = "duration") {
  
  # Helper to convert different formats to POSIXct
  to_time <- function(x) {
    if (inherits(x, "POSIXct")) {
      return(x)
    } else if (is.character(x)) {
      # Try parsing ISO datetime strings
      return(as.POSIXct(x, format = "%Y-%m-%dT%H:%M:%OS"))
    } else if (is.numeric(x)) {
      # Treat as Excel serial date (days since 1899-12-30)
      return(as.POSIXct(x * 86400, origin = "1899-12-30"))
    } else {
      stop("Unsupported time format in column.")
    }
  }
  
  
  df[[start_col]] <- to_time(df[[start_col]])
  df[[end_col]]   <- to_time(df[[end_col]])
  
  
  df[[output_col]] <- round(as.numeric(difftime(df[[end_col]], df[[start_col]], units = "mins")), 1)
  
  cat("\n", output_col, "column added. Unit: minutes\n")
  return(df)
}


#-----------------------------------------------------------------------------
# get.question.choices()
# Retrieves valid choice options for a select_one or select_multiple question.
# Looks up the related list_name in the choices sheet and returns options.
# Returns a character vector of choices or NA if not found.
#---------------------------------------------------------------------------

get.question.choices <- function(question_name, survey_df, choices_df) {
  # Check if the question exists in the survey data
  if (!question_name %in% survey_df$name) {
    warning(paste("Question", question_name, "not found in survey data"))
    return(NA)
  }
  
 
  question_type <- survey_df$type[survey_df$name == question_name]
  
  # Check if it's a select question type
  if (!grepl("^select_(one|multiple)", question_type)) {
    return(NA)
  }
  
  
  list_name <- sub("^select_(one|multiple)\\s+", "", question_type)
  cat(list_name, "\n")
  
  if (!list_name %in% choices_df$list_name) {
    warning(paste("List name", list_name, "not found in choices data for question", question_name))
    return(NA)
  }
  
  
  options <- choices_df$name[choices_df$list_name == list_name]
  
  
  if (length(options) == 0) {
    warning(paste("No options found for list name", list_name, "in question", question_name))
    return(NA)
  }
  
  # Return as a character vector
  return(as.character(options))
}


# identical wit add.audit.movement()
# To do: need to be unified later
#-----------------------------------------------------------------------------
# calculate.distance()
# Computes distances between two sets of latitude/longitude columns.
# Supports geodesic or haversine methods and vectorized computation.
# Adds the calculated distance as a new column in the dataframe.
#-----------------------------------------------------------------------------
calculate.distance <- function(df,
                               lat1,
                               lon1,
                               lat2,
                               lon2,
                               method = c("geodesic", "haversine"),
                               digits = 0,
                               col_name) {
  
  if (!requireNamespace("geosphere", quietly = TRUE)) {
    stop("Package 'geosphere' is required but not installed.")
  }
  
  # Validate method argument
  method <- match.arg(method)
  dist_function <- switch(
    method,
    geodesic = geosphere::distGeo,
    haversine = geosphere::distHaversine
  )
  
  # Check that columns exist
  required_cols <- c(lat1, lon1, lat2, lon2)
  missing_cols <- setdiff(required_cols, colnames(df))
  if (length(missing_cols) > 0) {
    stop(
      "Error: The following specified columns do not exist in the dataframe: ",
      paste(missing_cols, collapse = ", ")
    )
  }
  
  
  if (!is.character(col_name) || length(col_name) != 1 || nchar(col_name) == 0) {
    stop("col_name must be a non-empty character string")
  }
  
  
  if (col_name %in% colnames(df)) {
    warning("Column '", col_name, "' already exists in dataframe. It will be overwritten.")
  }
  
  
  result <- dplyr::as_tibble(df) %>%
    dplyr::mutate(dplyr::across(dplyr::all_of(required_cols), as.numeric))
  
  # Create the matrices of points using proper column referencing
  lon1_vec <- dplyr::pull(result, !!rlang::sym(lon1))
  lat1_vec <- dplyr::pull(result, !!rlang::sym(lat1))
  lon2_vec <- dplyr::pull(result, !!rlang::sym(lon2))
  lat2_vec <- dplyr::pull(result, !!rlang::sym(lat2))
  
  # Create matrices in correct order: longitude first, then latitude
  mat1 <- cbind(lon1_vec, lat1_vec)
  mat2 <- cbind(lon2_vec, lat2_vec)
  
  # Calculate distances all at once (vectorized - much faster)
  dist_vec <- dist_function(mat1, mat2)
  
  
  result[[col_name]] <- round(dist_vec, digits)
  cat("\n", col_name, "column added. Unit is meter.\n")
  return(result)
}

 
