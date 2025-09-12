#-----------------------------------------------------------------------------
# start.log()
# Initializes a loggable object containing the dataset and an empty log table.
# This sets up the structure for tracking data issues or checks.
#-----------------------------------------------------------------------------
start.log <- function(df) {
  structure(list(
    data = df,
    logs = tibble(
      uuid = character(),
      old_value = character(),
      question = character(),
      issue = character(),
      check_id = character(),
      check_binding = character()
    )
  ), class = "loggable")
}

#-----------------------------------------------------------------------------
# finish.logs()
# Extracts and formats the logged issues from a loggable object.
# Adds placeholders for new values and organizes the logs.
#-----------------------------------------------------------------------------
finish.logs <- function(log_obj) {
  stopifnot(inherits(log_obj, "loggable"))
  
  log_obj$logs %>%
    select(uuid, old_value, question, issue, check_id, check_binding) %>%
    arrange(check_binding) %>%
    mutate(
      change_type = NA_character_,
      new_value = NA_character_
    )
}


#-----------------------------------------------------------------------------
# parse_any_time() 
# # helper to safely parses different time formats into POSIXct.
# Handles numeric timestamps, character strings, and Date/POSIXt objects.
#-----------------------------------------------------------------------------
parse_any_time <- function(x) {
  if (is.null(x) || is.na(x)) return(NA)
  
  # numeric (Unix timestamp: seconds or milliseconds)
  if (is.numeric(x)) {
    return(as.POSIXct(ifelse(x > 1e12, x/1000, x),
                      origin = "1970-01-01", tz = "UTC"))
  }
  
  # character input
  if (is.character(x)) {
    suppressWarnings({
      parsed <- parse_date_time(x, 
                                orders = c(
                                  "Ymd HMS", "Ymd HM", "Ymd H", "Ymd",
                                  "dmY HMS", "dmY HM", "dmY",
                                  "HMS", "HM", "H"      # time-only
                                ),
                                tz = "UTC",
                                truncated = 3
      )
    })
    return(parsed)
  }
  
  # Already datetime
  if (inherits(x, "POSIXt") || inherits(x, "Date")) {
    return(as.POSIXct(x, tz = "UTC"))
  }
  
  return(NA)
}

#-----------------------------------------------------------------------------
# check.timeframe()
# Logs rows where a start/end time falls outside a specified timeframe.
# Uses robust datetime parsing and adds formatted log entries for violations.
#-----------------------------------------------------------------------------
check.timeframe <- function(log_obj, start_col = "start", end_col = "end", 
                            uuid_column = "_uuid",
                            from, to = Sys.time(),
                            message = "Outside allowed timeframe",
                            type = "timeframe") {
  stopifnot(inherits(log_obj, "loggable"))
  df <- log_obj$data
  

  from <- parse_any_time(from)
  to <- parse_any_time(to)
  
  starts <- sapply(df[[start_col]], parse_any_time)
  ends <- sapply(df[[end_col]], parse_any_time)
  
  starts <- as.POSIXct(starts, origin = "1970-01-01", tz = "UTC")
  ends <- as.POSIXct(ends, origin = "1970-01-01", tz = "UTC")
  
  # Skip cases where start > end (invalid duration)
  valid_duration <- is.na(starts) | is.na(ends) | starts <= ends
  
  # find violations (only for valid duration cases)
  bad_idx <- which(
    valid_duration & (  # Only check timeframe for valid durations
      (!is.na(starts) & starts < from) |
        (!is.na(ends) & ends > to)
    )
  )
  
  if (length(bad_idx) == 0) return(log_obj)
  
  # helper safe formatter
  safe_fmt <- function(x) {
    sapply(x, function(dt) {
      if (is.na(dt)) {
        return(NA_character_)
      } else {
        return(format(dt, "%Y-%m-%d %H:%M:%S"))
      }
    })
  }
  
  # Format the values for the bad indices only
  formatted_starts <- safe_fmt(starts[bad_idx])
  formatted_ends <- safe_fmt(ends[bad_idx])
  
  # new logs in the same structure as check.text
  new_logs <- tibble(
    uuid = df[[uuid_column]][bad_idx],
    old_value = formatted_starts,
    question = start_col,
    issue = paste0(
      message, ": [",
      formatted_starts, " → ", formatted_ends,
      "]"
    ),
    check_id = type,
    check_binding = stri_rand_strings(length(bad_idx), 10, pattern = "[a-z0-9]")
  )
  
  
  log_obj$logs <- bind_rows(log_obj$logs, new_logs)
  log_obj
}

#-----------------------------------------------------------------------------
# check.time.proximity()
# Detects and logs rows where consecutive entries by an enumerator are 
# too close in time.
# Calculates gaps between end and next start times and records any violations.
#-----------------------------------------------------------------------------
check.time.proximity <- function(log_obj, start_col = "start", end_col = "end",
                                 enumerator_col,
                                 uuid_column = "_uuid",
                                 min_gap_mins = 10,
                                 message = "Time distance issue", 
                                 type = "time_proximity") {
  stopifnot(inherits(log_obj, "loggable"))
  
  df <- log_obj$data
  stopifnot(all(c(start_col, end_col, enumerator_col, uuid_column) %in% names(df)))
  
  # Robust datetime parser
  parse_any_datetime <- function(x) {
    if (is.numeric(x)) {
      if (max(x, na.rm = TRUE) > 1e12) {
        as.POSIXct(x / 1000, origin = "1970-01-01")
      } else {
        as.POSIXct(x, origin = "1970-01-01")
      }
    } else if (inherits(x, "POSIXct")) {
      x
    } else if (is.character(x)) {
      suppressWarnings(
        parsed <- lubridate::parse_date_time(x, orders = c(
          "Y-m-d H:M:S", "Y/m/d H:M:S",
          "Y-m-d I:M:S p", "Y/m/d I:M:S p",
          "Y-m-d H:M", "Y/m/d H:M",
          "d/m/Y H:M:S", "d-m-Y H:M:S"
        ))
      )
      parsed
    } else {
      NA
    }
  }
  
  df <- df %>%
    mutate(
      start_parsed = parse_any_datetime(.data[[start_col]]),
      end_parsed   = parse_any_datetime(.data[[end_col]])
    )
  
  # Arrange and calculate gaps
  df <- df %>%
    arrange(.data[[enumerator_col]], start_parsed) %>%
    group_by(.data[[enumerator_col]]) %>%
    mutate(
      next_start = lead(start_parsed),
      next_uuid  = lead(.data[[uuid_column]])
    ) %>%
    ungroup()
  
  flagged <- df %>%
    mutate(time_gap_mins = as.numeric(difftime(next_start, end_parsed, units = "mins"))) %>%
    filter(!is.na(time_gap_mins) & time_gap_mins < min_gap_mins)
  
  if (nrow(flagged) == 0) return(log_obj)
  
  # Generate logs safely
  new_logs <- flagged %>%
    rowwise() %>%
    mutate(pair_key = stringi::stri_rand_strings(1, 10, pattern = "[a-z0-9]")) %>%
    do({
      current <- .
      tibble(
        uuid = c(current[[uuid_column]], current$next_uuid),
        old_value = c(as.character(current[[start_col]]), as.character(df[[start_col]][df[[uuid_column]] == current$next_uuid][1])),
        question = start_col,
        issue = paste0(message, " | Gap = ", round(current$time_gap_mins, 2), " mins"),
        check_id = type,
        check_binding = current$pair_key
      )
    })
  
  log_obj$logs <- bind_rows(log_obj$logs, new_logs)
  log_obj
}

# -------------------------------------------------------------------
# check.text()
# Logs non-empty text responses for selected columns.
# Useful for translation or harmonization of free-text answers.
# Skips columns not found in the dataset or with only empty/NA values.
# --------------------------------------------------------------------

check.text <- function(log_obj, uuid_column = "_uuid", text_columns, 
                     message = "Translate/harmonize", type = "text") {
  stopifnot(inherits(log_obj, "loggable"))
  df <- log_obj$data
  
  text_columns <- intersect(text_columns, names(df))
  if (length(text_columns) == 0) return(log_obj)
  
  new_logs <- map_dfr(text_columns, function(col_name) {
    df %>%
      filter(!is.na(.data[[col_name]]) & .data[[col_name]] != "") %>%
      transmute(
        uuid = .data[[uuid_column]],
        old_value = as.character(.data[[col_name]]),
        question = col_name,
        issue = message,
        check_id = type,
        check_binding = stri_rand_strings(n(), 10, pattern = "[a-z0-9]")
      )
  })
  
  log_obj$logs <- bind_rows(log_obj$logs, new_logs)
  log_obj
}

#-----------------------------------------------------------------------------
# check.soft.duplicates()
# Logs potential duplicates based on similarity percentages between rows.
# Ensures input is valid and generates logs describing soft duplicate pairs.
#-----------------------------------------------------------------------------
check.soft.duplicates <- function(log_obj, similarity_result, uuid_column = "_uuid",
                                  message = "Soft duplicate with",
                                  type = "soft_duplicate") {
  stopifnot(inherits(log_obj, "loggable"))
  
  # Check if similarity_result is a list with similarity_percentage
  if (!is.list(similarity_result) || !"similarity_percentage" %in% names(similarity_result)) {
    warning("similarity_result must be a list containing 'similarity_percentage'. Nothing to log.")
    return(log_obj)
  }
  
  sim_df <- similarity_result$similarity_percentage
  
  # Check if similarity_percentage is a valid data frame with rows
  if (!is.data.frame(sim_df) || nrow(sim_df) == 0) {
    warning("'similarity_percentage' is not a valid data frame or has no rows. Nothing to log.")
    return(log_obj)
  }
  
  required_cols <- c("uuid1", "uuid2", "similarity_percentage")
  if (!all(required_cols %in% names(sim_df))) {
    warning("'similarity_percentage' is missing required columns. Nothing to log.")
    return(log_obj)
  }
  
  new_logs <- sim_df %>%
    transmute(
      uuid = uuid2,
      old_value = NA_character_,
      question = uuid_column,
      issue = paste0(message, " ", uuid1, " (", similarity_percentage, "%)"),
      check_id = type,
      check_binding = stringi::stri_rand_strings(nrow(.), 10, pattern = "[a-z0-9]")
    )
  
  # Append to log object
  log_obj$logs <- bind_rows(log_obj$logs, new_logs)
  log_obj
}

#-----------------------------------------------------------------------------
# check.proximity()
# Logs entries where two points are closer than a given distance threshold.
# Creates logs for both points in each flagged pair with distance information.
#-----------------------------------------------------------------------------
check.proximity <- function(log_obj, proxinity_data, threshold = 50, uuid_col = "_uuid",
                            message = "Submissions are too close", type = "validation") {
  
  stopifnot(inherits(log_obj, "loggable"))
  
  distance_col <- "distance_m"
  uuid1_col <- "uuid1"
  uuid2_col <- "uuid2"
  
  flagged <- proxinity_data %>% filter(.data[[distance_col]] < threshold)
  if (nrow(flagged) == 0) return(log_obj)
  
  flagged <- flagged %>%
    rowwise() %>%
    mutate(key = stri_rand_strings(1, 10, pattern = "[a-z0-9]")) %>%
    ungroup()
  
  # Create logs for both uuids in the pair with distance in message
  new_logs <- bind_rows(
    flagged %>% transmute(
      uuid = .data[[uuid1_col]],
      old_value = NA_character_,
      question = uuid_col,
      issue = paste0(message, " (distance: ", round(.data[[distance_col]], 2), " m)"),
      check_id = type,
      check_binding = key
    ),
    flagged %>% transmute(
      uuid = .data[[uuid2_col]],
      old_value = NA_character_,
      question = uuid_col,
      issue = paste0(message, " (distance: ", round(.data[[distance_col]], 2), " m)"),
      check_id = type,
      check_binding = key
    )
  )
  
  log_obj$logs <- bind_rows(log_obj$logs, new_logs)
  log_obj
}

#-----------------------------------------------------------------------------
# check.overlap.time()
# Logs rows where time intervals overlap with another row.
# Ensures required columns exist and records each overlap for review.
#-----------------------------------------------------------------------------
check.overlap.time <- function(log_obj, overlap_result,
                               message = "Time overlapping with",
                               type = "overlap_time") {
  stopifnot(inherits(log_obj, "loggable"))
  
  if (!is.data.frame(overlap_result) || nrow(overlap_result) == 0) {
    warning("overlap_result must be a non-empty data frame. Nothing to log.")
    return(log_obj)
  }
  
  required_cols <- c("uuid1", "uuid2", "col_start")
  if (!all(required_cols %in% names(overlap_result))) {
    warning("overlap_result is missing required columns. Nothing to log.")
    return(log_obj)
  }
  
  overlap_result <- overlap_result %>% select(-1)
  overlap_result <- overlap_result %>% select(all_of(required_cols))

  new_logs <- overlap_result %>%
    transmute(
      uuid = uuid2,
      old_value = NA_character_,
      question = col_start,
      issue = paste0(message, " ", uuid1),
      check_id = type,
      check_binding = stringi::stri_rand_strings(nrow(.), 10, pattern = "[a-z0-9]")
    )
  
  log_obj$logs <- bind_rows(log_obj$logs, new_logs)
  log_obj
}

#-----------------------------------------------------------------------------
# check.outliers()
# Identifies numeric outliers in a dataset using normal or log distribution.
# Logs flagged outlier values per column with details for each UUID.
#-----------------------------------------------------------------------------
check.outliers <- function(log_obj,
                           uuid_column = "_uuid",
                           numeric_columns = NULL,
                           strongness_factor = 3,
                           minimum_unique_values = NULL,
                           type = "outlier") {
  

  stopifnot(inherits(log_obj, "loggable"))
  df <- log_obj$data
  
  if (!uuid_column %in% names(df)) {
    stop("UUID column not found in dataset.")
  }
  
  # Identify numeric columns to check
  if (is.null(numeric_columns)) {
    numeric_columns <- df %>%
      dplyr::select_if(is.numeric) %>%
      names()
  } else {

    missing_cols <- setdiff(numeric_columns, names(df))
    if (length(missing_cols) > 0) {
      stop("The following numeric columns were not found: ", paste(missing_cols, collapse = ", "))
    }
    
    non_numeric_cols <- numeric_columns[!sapply(df[numeric_columns], is.numeric)]
    if (length(non_numeric_cols) > 0) {
      stop("The following columns are not numeric: ", paste(non_numeric_cols, collapse = ", "))
    }
  }
  
  # Remove UUID from numeric columns if present
  numeric_columns <- setdiff(numeric_columns, uuid_column)
  
  if (length(numeric_columns) == 0) {
    warning("No numeric columns found to check for outliers.")
    return(log_obj)
  }
  
  # Check for outliers in each numeric column
  new_logs <- purrr::map_dfr(numeric_columns, function(col) {
    
    values <- df[[col]]
    values <- values[!is.na(values) & is.finite(values)]
    
    if (length(values) < 2) {
      warning(paste("Skipping", col, "- insufficient data"))
      return(NULL)
    }
    
    if (!is.null(minimum_unique_values) && length(unique(values)) < minimum_unique_values) {
      return(NULL)
    }
    
    outlier_entries <- list()
    
    # Check for normal distribution outliers
    mean_val <- mean(values)
    sd_val <- stats::sd(values)
    outliers_normal <- abs(values - mean_val) > strongness_factor * sd_val
    
    if (any(outliers_normal)) {
      outlier_values <- unique(values[outliers_normal])
      
      normal_outliers <- df %>%
        dplyr::filter(.data[[col]] %in% outlier_values) %>%
        dplyr::transmute(
          uuid = .data[[uuid_column]],
          old_value = as.character(.data[[col]]),
          question = col,
          issue = "normal distribution outlier",
          check_id = type,
          check_binding = stringi::stri_rand_strings(nrow(.), 10, pattern = "[a-z0-9]")
        )
      
      outlier_entries <- c(outlier_entries, list(normal_outliers))
    }
    
    # Check for log distribution outliers (for positive values only)
    if (all(values >= 0, na.rm = TRUE) && any(values > 0, na.rm = TRUE)) {
      log_values <- log(values + 1)
      log_mean <- mean(log_values)
      log_sd <- stats::sd(log_values)
      outliers_log <- abs(log_values - log_mean) > strongness_factor * log_sd
      
      if (any(outliers_log)) {
        outlier_log_values <- unique(values[outliers_log])
        
        log_outliers <- df %>%
          dplyr::filter(.data[[col]] %in% outlier_log_values) %>%
          dplyr::transmute(
            uuid = .data[[uuid_column]],
            old_value = as.character(.data[[col]]),
            question = col,
            issue = "log distribution outlier",
            check_id = type,
            check_binding = stringi::stri_rand_strings(nrow(.), 10, pattern = "[a-z0-9]")
          )
        
        outlier_entries <- c(outlier_entries, list(log_outliers))
      }
    }
    
    # Combine results for this column
    if (length(outlier_entries) > 0) {
      dplyr::bind_rows(outlier_entries)
    } else {
      NULL
    }
  })
  
  if (nrow(new_logs) > 0) {
    new_logs <- new_logs %>%
      dplyr::distinct(uuid, question, old_value, .keep_all = TRUE)
    
    log_obj$logs <- dplyr::bind_rows(log_obj$logs, new_logs)
  }
  
  return(log_obj)
}

#-----------------------------------------------------------------------------
# extract.other.variables()
# Extracts valid "_other" survey variables linked to select_one or 
# select_multiple questions.
# Helps identify optional "other" fields for further checks.
#-----------------------------------------------------------------------------
extract.other.variables <- function(survey_df) {
  if (!all(c("type", "name") %in% names(survey_df))) {
    stop("Dataframe must contain 'type' and 'name' columns")
  }
  
  other_vars <- survey_df$name[grepl("_other$", survey_df$name)]
  valid_other_vars <- character(0)
  
  for (other_var in other_vars) {
    parent_var <- sub("_other$", "", other_var)
    if (parent_var %in% survey_df$name) {
      parent_type <- survey_df$type[survey_df$name == parent_var]
      if (grepl("^select_one|^select_multiple", parent_type)) {
        valid_other_vars <- c(valid_other_vars, other_var)
      }
    }
  }
  
  valid_other_vars
}

# -------------------------------------------------------------------
# Internal helper: process one column robustly
# -------------------------------------------------------------------
.process_other_column <- function(df, uuid_column, col_name, message, type ="other_specify") {
  df %>%
    select(all_of(c(uuid_column, col_name))) %>%
    mutate(
      old_value = as.character(.data[[col_name]]),
      question  = col_name,
      issue = if (!is.null(message)) message else NA_character_,
      check_id = type,
      check_binding = stringi::stri_rand_strings(nrow(.), 10, pattern = "[a-z0-9]")
    ) %>%
    filter(!is.na(old_value) & old_value != "") %>%
    select(uuid = all_of(uuid_column), old_value, question, issue, check_id, check_binding)
}

#-----------------------------------------------------------------------------
# check.others()
# Logs all valid "_other" columns from a survey dataset into a loggable object.
# Uses helper functions to process each column and append results to logs.
#-----------------------------------------------------------------------------
check.others <- function(log_obj, survey_df, uuid_column = "_uuid", 
                       message = "Check and harmonize", type = "other_specify") {
  stopifnot(inherits(log_obj, "loggable"))
  df <- log_obj$data
  
  other_columns <- extract.other.variables(survey_df)
  other_columns <- intersect(other_columns, names(df))
  
  if (length(other_columns) == 0) return(log_obj)
  
  new_logs <- map_dfr(other_columns, ~.process_other_column(df, uuid_column, .x, message, type))
  log_obj$logs <- bind_rows(log_obj$logs, new_logs)
  log_obj
}

#-----------------------------------------------------------------------------
# check.others.for.columns()
# Logs specified "_other" columns explicitly provided by the user.
# Filters non-existing columns and appends structured logs for review.
#-----------------------------------------------------------------------------
check.others.for.columns <- function(log_obj, uuid_column = "_uuid", other_columns,
                                   message = "Check and harmonize", type = "other_specify") {
  stopifnot(inherits(log_obj, "loggable"))
  df <- log_obj$data
  
  other_columns <- intersect(other_columns, names(df))
  if (length(other_columns) == 0) return(log_obj)
  
  new_logs <- map_dfr(other_columns, ~.process_other_column(df, uuid_column, .x, message, type))
  log_obj$logs <- bind_rows(log_obj$logs, new_logs)
  log_obj
}

# --------------------------------------------------------------------
# check.if()
#
# Logs rows where a condition (on one or more columns) is TRUE.
# Returns uuid, column name(s), value, message, and type.
# If multi_logs = TRUE: creates one log row with combined values,
# If multi_logs = FALSE: creates multiple lines log row per variable.
# --------------------------------------------------------------------
check.if <- function(log_obj, condition, uuid_column = "_uuid",
                     message = "Check data", type = "validation",
                     multi_logs = TRUE, cols = NULL) {
  stopifnot(inherits(log_obj, "loggable"))
  
  df <- log_obj$data
  condition_expr <- tryCatch(enquo(condition), 
                             error = function(e) {
                               warning("Invalid condition expression: ", e$message)
                               return(NULL)
                             })
  if (is.null(condition_expr)) return(log_obj)
  
  condition_vars <- all.vars(condition_expr)
  
  # Check if condition vars exist
  missing_vars <- setdiff(condition_vars, names(df))
  if (length(missing_vars) > 0) {
    warning("Variables not found in data: ", paste(missing_vars, collapse = ", "))
    return(log_obj)
  }
  
  # Filter rows matching condition
  flagged <- tryCatch(df %>% filter(!!condition_expr),
                      error = function(e) {
                        warning("Condition evaluation failed: ", e$message)
                        return(df[0, ])
                      })
  if (nrow(flagged) == 0) return(log_obj)
  
  uuid_keys <- flagged %>%
    distinct(.data[[uuid_column]]) %>%
    mutate(key = stri_rand_strings(n(), 10, pattern = "[a-z0-9]"))
  
  # If cols parameter is given
  if (!is.null(cols) && length(cols) > 0) {
    # keep only existing columns
    missing_cols <- setdiff(cols, names(df))
    if (length(missing_cols) > 0) {
      warning("Columns not found in data: ", paste(missing_cols, collapse = ", "))
      cols <- setdiff(cols, missing_cols)
    }
    if (length(cols) == 0) return(log_obj)
    
    # Build condition-based issue message (same for all logs of same uuid)
    flagged <- flagged %>%
      left_join(uuid_keys, by = uuid_column) %>%
      mutate(
        issue_msg = purrr::pmap_chr(
          across(all_of(condition_vars)),
          function(...) {
            vals <- c(...)
            pairs <- paste0(condition_vars, ": ", as.character(vals))
            paste0(message, " | ", paste(pairs, collapse = ", "))
          }
        )
      )
    
    # Create logs for condition vars + extra cols
    all_cols <- unique(c(condition_vars, cols))
    
    new_logs <- map_dfr(all_cols, function(var) {
      flagged %>%
        transmute(
          uuid = .data[[uuid_column]],
          old_value = as.character(.data[[var]]),
          question = var,
          issue = issue_msg,
          check_id = type,
          check_binding = key
        )
    }) %>% distinct()
    
  } else if (multi_logs) {
    # One row per condition variable
    new_logs <- map_dfr(condition_vars, function(var) {
      flagged %>%
        left_join(uuid_keys, by = uuid_column) %>%
        transmute(
          uuid = .data[[uuid_column]],
          old_value = as.character(.data[[var]]),
          question = var,
          issue = paste0(message, " | ", var, ": ", as.character(.data[[var]])),
          check_id = type,
          check_binding = key
        )
    }) %>% distinct()
    
  } else {
    # One row per UUID with combined condition message
    new_logs <- flagged %>%
      left_join(uuid_keys, by = uuid_column) %>%
      mutate(
        combined_msg = purrr::pmap_chr(
          across(all_of(condition_vars)),
          function(...) {
            vals <- c(...)
            pairs <- paste0(condition_vars, ": ", as.character(vals))
            paste0(message, " | ", paste(pairs, collapse = ", "))
          }
        )
      ) %>%
      transmute(
        uuid = .data[[uuid_column]],
        old_value = as.character(.data[[condition_vars[1]]]),
        question = condition_vars[1],
        issue = combined_msg,
        check_id = type,
        check_binding = stri_rand_strings(n(), 10, pattern = "[a-z0-9]")
      ) %>% distinct()
  }
  
  log_obj$logs <- bind_rows(log_obj$logs, new_logs)
  log_obj 
}

