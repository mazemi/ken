
#-----------------------------------------------------------------------------
# detect.nearby()
# Detects pairs of points in a dataframe that are within a specified distance.
# Optionally filters by same enumerator and includes additional columns in output.
# Returns a sorted dataframe of nearby pairs with distances and extra info.
#-----------------------------------------------------------------------------
detect.nearby <- function(df,
                          uuid_col = "_uuid",
                          enumerator_col = NULL,
                          additional_cols = NULL,
                          longitude_col = "_gps_longitude",
                          latitude_col = "_gps_latitude",
                          min_distance,
                          same_enumerator = TRUE) {

  if (!is.data.frame(df)) stop("df must be a data frame")
  if (nrow(df) < 2) stop("Data frame must contain at least 2 rows")
  if (!uuid_col %in% names(df)) stop("uuid_col '", uuid_col, "' not found in data frame")
  if (!longitude_col %in% names(df)) stop("longitude_col '", longitude_col, "' not found in data frame")
  if (!latitude_col %in% names(df)) stop("latitude_col '", latitude_col, "' not found in data frame")
  if (!is.numeric(min_distance) || min_distance <= 0) stop("min_distance must be a positive number")

  missing_coords <- is.na(df[[longitude_col]]) | is.na(df[[latitude_col]])
  if (any(missing_coords)) {
    warning("Removing ", sum(missing_coords), " rows with missing coordinates")
    df <- df[!missing_coords, ]
  }
  if (nrow(df) < 2) stop("Not enough rows with valid coordinates (need at least 2)")

  # Ensure numeric coords
  df[[longitude_col]] <- as.numeric(df[[longitude_col]])
  df[[latitude_col]]  <- as.numeric(df[[latitude_col]])

  if (!is.null(additional_cols)) {
    missing_add_cols <- setdiff(additional_cols, names(df))
    if (length(missing_add_cols) > 0) stop("Additional columns not found: ", paste(missing_add_cols, collapse = ", "))
  }

  # Keep only necessary columns
  keep_cols <- c(uuid_col, longitude_col, latitude_col)
  if (!is.null(additional_cols)) keep_cols <- c(keep_cols, additional_cols)
  if (!is.null(enumerator_col)) keep_cols <- c(keep_cols, enumerator_col)
  df <- df[, keep_cols, drop = FALSE]

  coords <- df[, c(longitude_col, latitude_col)]
  sp_points <- sp::SpatialPoints(coords, proj4string = sp::CRS("+proj=longlat +datum=WGS84"))

  # Calculate distance matrix in meters
  dist_matrix <- geosphere::distm(sp_points, fun = geosphere::distHaversine)

  pairs <- which(dist_matrix <= min_distance & dist_matrix > 0, arr.ind = TRUE)
  if (nrow(pairs) == 0) {
    message("No pairs found within ", min_distance, " meters")
    return(data.frame())
  }

  # Create result dataframe
  result <- data.frame(
    uuid1 = df[pairs[, 1], ][[uuid_col]],
    uuid2 = df[pairs[, 2], ][[uuid_col]],
    distance_m = dist_matrix[pairs],
    stringsAsFactors = FALSE
  )

  if (!is.null(additional_cols)) {
    for (col in additional_cols) {
      result[[paste0(col, "_1")]] <- df[pairs[, 1], col]
      result[[paste0(col, "_2")]] <- df[pairs[, 2], col]
    }
  }

  # Add enumerator columns if specified
  if (!is.null(enumerator_col)) {
    result[[paste0(enumerator_col, "_1")]] <- df[pairs[, 1], enumerator_col]
    result[[paste0(enumerator_col, "_2")]] <- df[pairs[, 2], enumerator_col]
  }

  result <- result[order(result$distance_m), ]

  if (!is.null(enumerator_col) && same_enumerator) {
    col1 <- paste0(enumerator_col, "_1")
    col2 <- paste0(enumerator_col, "_2")

    result <- result %>%
      dplyr::filter(as.vector(.data[[col1]] == .data[[col2]])) %>%
      dplyr::select(
        uuid1, uuid2, distance_m,
        !!enumerator_col := dplyr::all_of(col1),
        dplyr::everything()
      )
  }

  return(result)
}

#-----------------------------------------------------------------------------
# add.audit.gps()
# Reads audit CSV files and extracts GPS information for each UUID.
# Computes max movement, centroid, accuracy stats, and missing GPS counts.
# Joins GPS audit data with the main dataframe.
#-----------------------------------------------------------------------------

add.audit.gps <- function(df, uuid_column = "_uuid", audit_path) {

  # Validate input parameters
  if (!is.data.frame(df)) {
    stop("Input 'df' must be a data frame")
  }

  if (!uuid_column %in% names(df)) {
    stop("UUID column '", uuid_column, "' not found in the data frame")
  }

  if (!dir.exists(audit_path)) {
    stop("Audit directory '", audit_path, "' does not exist")
  }

  # Find all audit.csv files recursively
  files <- tryCatch({
    list.files(path = audit_path, pattern = "audit\\.csv$", recursive = TRUE, full.names = TRUE)
  }, error = function(e) {
    stop("Error accessing audit directory: ", e$message)
  })

  if (length(files) == 0) {
    warning("No audit.csv files found in the specified directory")
    return(df)
  }

  results <- list()
  n_files <- length(files)
  processed_count <- 0
  skipped_count <- 0

  for (i in seq_along(files)) {
    file <- files[i]
    uuid <- basename(dirname(file))

    percent <- round(i / n_files * 100)
    cat(sprintf("\rProcessing: %d%% (%d of %d)", percent, i, n_files))
    flush.console()

    audit_df <- tryCatch({
      read_csv(file, show_col_types = FALSE, progress = FALSE)
    }, error = function(e) {
      warning("Failed to read file: ", file, " - ", e$message)
      return(NULL)
    })

    if (is.null(audit_df)) {
      skipped_count <- skipped_count + 1
      next
    }

    audit_df <- tryCatch({
      janitor::clean_names(audit_df)
    }, error = function(e) {
      warning("Failed to clean column names for file: ", file, " - ", e$message)
      skipped_count <- skipped_count + 1
      next
    })

    if (!all(c("latitude", "longitude") %in% names(audit_df))) {
      warning("Skipping file ", file, " - latitude/longitude columns not found")
      skipped_count <- skipped_count + 1
      next
    }

    df_valid <- audit_df %>% filter(!is.na(latitude) & !is.na(longitude))

    if (nrow(df_valid) == 0) {
      warning("Skipping file ", file, " - no valid GPS coordinates found")
      skipped_count <- skipped_count + 1
      next
    }

    # Calculate maximum distance with error handling
    max_distance <- tryCatch({
      coords <- df_valid[, c("longitude", "latitude")]
      dist_matrix <- distm(coords, fun = distHaversine)
      max(dist_matrix)
    }, error = function(e) {
      warning("Error calculating distance matrix for file: ", file, " - ", e$message)
      NA_real_
    })

    # Calculate centroid with error handling
    centroid <- tryCatch({
      c(latitude = mean(df_valid$latitude, na.rm = TRUE),
        longitude = mean(df_valid$longitude, na.rm = TRUE))
    }, error = function(e) {
      warning("Error calculating centroid for file: ", file, " - ", e$message)
      c(latitude = NA_real_, longitude = NA_real_)
    })

    # Accuracy stats (if accuracy column exists)
    min_acc <- tryCatch({
      if ("accuracy" %in% names(df_valid) && any(!is.na(df_valid$accuracy))) {
        min(df_valid$accuracy, na.rm = TRUE)
      } else {
        NA_real_
      }
    }, error = function(e) {
      warning("Error calculating min accuracy for file: ", file, " - ", e$message)
      NA_real_
    })

    max_acc <- tryCatch({
      if ("accuracy" %in% names(df_valid) && any(!is.na(df_valid$accuracy))) {
        max(df_valid$accuracy, na.rm = TRUE)
      } else {
        NA_real_
      }
    }, error = function(e) {
      warning("Error calculating max accuracy for file: ", file, " - ", e$message)
      NA_real_
    })

    # Count NA GPS values
    gps_na <- tryCatch({
      if (all(c("event", "latitude", "longitude") %in% names(audit_df))) {
        audit_df %>%
          filter(event %in% c("question", "group questions")) %>%
          summarise(count = sum(is.na(latitude) | is.na(longitude))) %>%
          pull(count)
      } else {
        NA_integer_
      }
    }, error = function(e) {
      warning("Error counting NA GPS values for file: ", file, " - ", e$message)
      NA_integer_
    })

    # Append to results
    results[[length(results) + 1]] <- data.frame(
      uuid = uuid,
      audit_gps_maximum_movement = round(max_distance,1),
      audit_centroid_latitude = centroid["latitude"],
      audit_centroid_longitude = centroid["longitude"],
      audit_gps_min_accuracy = min_acc,
      audit_gps_max_accuracy = max_acc,
      audit_no_gps = gps_na,
      stringsAsFactors = FALSE
    )

    processed_count <- processed_count + 1
  }

  cat("\n\nProcessing complete. Processed:", processed_count, "files, Skipped:", skipped_count, "files\n")

  # Combine all results
  if (length(results) == 0) {
    warning("No valid audit files were processed successfully")
    return(df)
  }

  gps_info <- tryCatch({
    bind_rows(results)
  }, error = function(e) {
    stop("Error combining results: ", e$message)
  })

  # Join with original dataframe
  final_df <- tryCatch({
    df %>% left_join(gps_info, by = setNames("uuid", uuid_column))
  }, error = function(e) {
    stop("Error joining GPS information with main dataframe: ", e$message)
  })

  return(final_df)
}


#-----------------------------------------------------------------------------
# add.audit.duration()
# Reads audit CSV files and calculates interview durations and delays.
# Summarizes long questions and optionally counts edits made during interviews.
# Merges audit duration and edit info with the main dataframe.
#-----------------------------------------------------------------------------

add.audit.duration <- function(df, audit_path, uuid_column = "_uuid",
                           compute_edits = TRUE,
                           verbose = TRUE,
                           return_counts_only = FALSE,
                           long_question_threshold = 5) {

  corrupted_files <- character(0)

  # --------------------------
  # Helper: process audit.csv
  # --------------------------
  process_audit_file <- function(file_path) {
    audit_data <- tryCatch({
      read_csv(file_path, col_types = cols(
        event = col_character(),
        start = col_double(),
        end = col_double(),
        node = col_character()
      ))
    }, error = function(e) {
      if (verbose) message("Corrupted file skipped: ", file_path, " - ", e$message)
      corrupted_files <<- c(corrupted_files, path_file(file_path))
      return(NULL)
    })

    if (is.null(audit_data)) return(NULL)

    form_resumed <- any(audit_data$event == "form resume", na.rm = TRUE)

    if (nrow(audit_data) == 0) {
      return(list(filtered_data = NULL, total_time_minutes = NA_real_, form_exited = form_resumed))
    }

    first_start <- min(audit_data$start, na.rm = TRUE)
    last_end <- max(audit_data$end, na.rm = TRUE)
    total_time_minutes <- (last_end - first_start) / (1000 * 60)

    # Find all group nodes to exclude
    group_nodes <- audit_data %>%
      filter(event == "group questions", !is.na(node)) %>%
      pull(node)

    exclude_patterns <- paste0("^", gsub("([\\W])", "\\\\\\1", group_nodes), "/")

    filtered_data <- audit_data %>%
      filter(event %in% c("question", "group questions")) %>%
      filter(!(event == "question" & !is.na(node) &
                 Reduce(`|`, lapply(exclude_patterns, function(p) grepl(p, node))))) %>%
      mutate(
        duration_minutes = (end - start) / (1000 * 60),
        long_question = ifelse(duration_minutes > long_question_threshold, sub(".*/", "", node), NA_character_)
      ) %>%
      filter(!is.na(duration_minutes), duration_minutes >= 0)

    list(
      filtered_data = filtered_data,
      total_time_minutes = total_time_minutes,
      form_exited = form_resumed
    )
  }

  # --------------------------
  # Collect audit files
  # --------------------------
  audit_files <- dir_ls(audit_path, regexp = "audit\\.csv$", recurse = TRUE, type = "file")

  if (length(audit_files) == 0) {
    if (verbose) message("No audit.csv files found in the specified directory tree")
    return(df)
  }

  if (verbose) cat("Found", length(audit_files), "audit.csv files\n")

  # --------------------------
  # Duration & remarks summary with percentage
  # --------------------------
  duration_summary <- map_df(seq_along(audit_files), function(i) {
    audit_file <- audit_files[i]
    uuid <- path_dir(audit_file) %>% path_file()
    result <- process_audit_file(audit_file)

    pct <- sprintf("%3d%%", round(i / length(audit_files) * 100))
    cat("\rProcessing durations:", pct)
    flush.console()

    if (!is.null(result) && !is.null(result$filtered_data) && nrow(result$filtered_data) > 0) {
      interview_duration_minutes <- sum(result$filtered_data$duration_minutes, na.rm = TRUE)
      delayed_time_minutes <- max(0, result$total_time_minutes - interview_duration_minutes, na.rm = TRUE)

      long_questions <- unique(na.omit(result$filtered_data$long_question))
      audit_remarks <- if (length(long_questions) > 0) paste0("Long spent time, more than ",
                                                              long_question_threshold, " minutes: ",
                                                              paste(long_questions, collapse = "; ")) else NA_character_

      tibble(
        uuid = uuid,
        interview_duration_minutes = round(interview_duration_minutes, 1),
        delayed_time_minutes = round(delayed_time_minutes, 2),
        form_exited = result$form_exited,
        number_of_navigation = nrow(result$filtered_data),
        audit_remarks = audit_remarks
      )
    } else {
      tibble(
        uuid = uuid,
        interview_duration_minutes = NA_real_,
        delayed_time_minutes = NA_real_,
        form_exited = ifelse(!is.null(result), result$form_exited, FALSE),
        number_of_navigation = 0,
        audit_remarks = NA_character_
      )
    }
  })
  cat("\n")  # move to new line after percentage

  # --------------------------
  # Edit count summary with percentage
  # --------------------------
  edit_summary <- NULL
  if (compute_edits) {
    audit_files_df <- tibble(
      audit_file = audit_files,
      uuid = path_dir(audit_files) %>% path_file()
    )

    edit_summary <- map_df(seq_len(nrow(audit_files_df)), function(i) {
      audit_file <- audit_files_df$audit_file[i]
      uuid <- audit_files_df$uuid[i]

      pct <- sprintf("%3d%%", round(i / nrow(audit_files_df) * 100))
      cat("\rProcessing edits:    ", pct)
      flush.console()

      tryCatch({
        csv_df <- read_csv(audit_file, show_col_types = FALSE)
        old_value_col <- intersect(names(csv_df), c("old.value", "old-value", "old_value"))
        edit_count <- if (length(old_value_col) > 0) sum(!is.na(csv_df[[old_value_col[1]]]) & csv_df[[old_value_col[1]]] != "", na.rm = TRUE) else NA_integer_
        tibble(uuid = uuid, edit_count = edit_count)
      }, error = function(e) {
        corrupted_files <<- c(corrupted_files, path_file(audit_file))
        tibble(uuid = uuid, edit_count = NA_integer_)
      })
    }) %>%
      group_by(uuid) %>%
      summarise(edit_count = if (all(is.na(edit_count))) NA_integer_ else sum(edit_count, na.rm = TRUE), .groups = "drop")
    cat("\n")
  }


  final_summary <- duration_summary
  if (!is.null(edit_summary)) {
    final_summary <- full_join(final_summary, edit_summary, by = "uuid")
  }

  final_summary <- final_summary %>%
    select(-audit_remarks, everything(), audit_remarks)

  if (return_counts_only) {
    return(list(summary = final_summary, corrupted_files = corrupted_files))
  }

  if (!uuid_column %in% names(df)) {
    stop(paste("UUID column", uuid_column, "not found in main_data"))
  }

  result <- df %>%
    left_join(final_summary, by = setNames("uuid", uuid_column))

  main_uuids <- unique(df[[uuid_column]])
  audit_uuids <- unique(final_summary$uuid)
  shared_uuids <- intersect(main_uuids, audit_uuids)

  cat("\n------------------ Summary -----------------\n")
  cat("Total rows in main data:", nrow(df), "\n")
  cat("Total audit files found:", length(audit_files), "\n")
  cat("Total audits matching with data:", length(shared_uuids), "\n")
  if (length(corrupted_files) > 0) {
    cat("Corrupted/skipped audit files:", paste(corrupted_files, collapse = ", "), "\n")
  }
  cat("--------------------------------------------\n")

  return(result)
}

#-----------------------------------------------------------------------------
# add.audit.movement()
# Computes distance between two sets of latitude/longitude columns in a dataframe.
# Supports geodesic or haversine calculation methods
# Adds the calculated distance as a new column to the dataframe in meter.
#-----------------------------------------------------------------------------

#' Add audit info
#'
#' Enriches dataframe with audit logs (edits, gps, long question duration, etc.)
#'
#' @param df Dataframe to update.
#' @param audit_path Path to audit files.
#' @param uuid_column Character. Name of UUID column. Default = "_uuid".
#' @param compute_edits Logical. Compute edit counts? Default = TRUE.
#' @param compute_gps Logical. Extract GPS data? Default = FALSE.
#' @param long_question_threshold Numeric. Threshold in seconds for "long" questions. Default = 120.
#' @param verbose Logical. Show progress messages? Default = TRUE.
#'
#' @return Dataframe with extra audit info.
#' @export
add.audit.movement <- function(df,
                         lat1,
                         lon1,
                         lat2,
                         lon2,
                         method = c("geodesic", "haversine"),
                         digits = 0,
                         col_name = "distance_meter") {

  if (!requireNamespace("geosphere", quietly = TRUE)) {
    stop("Package 'geosphere' is required but not installed.")
  }

  method <- match.arg(method)
  dist_function <- switch(
    method,
    geodesic = geosphere::distGeo,
    haversine = geosphere::distHaversine
  )

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

  # Convert to tibble and ensure columns are numeric
  result <- dplyr::as_tibble(df) %>%
    dplyr::mutate(dplyr::across(dplyr::all_of(required_cols), as.numeric))

  lon1_vec <- dplyr::pull(result, !!rlang::sym(lon1))
  lat1_vec <- dplyr::pull(result, !!rlang::sym(lat1))
  lon2_vec <- dplyr::pull(result, !!rlang::sym(lon2))
  lat2_vec <- dplyr::pull(result, !!rlang::sym(lat2))

  mat1 <- cbind(lon1_vec, lat1_vec)
  mat2 <- cbind(lon2_vec, lat2_vec)

  dist_vec <- dist_function(mat1, mat2)

  # Add the result, rounded, to the dataframe with custom column name
  result[[col_name]] <- round(dist_vec, digits)
  cat("\n", col_name, "column added. Unit is meter.\n")
  return(result)
}


add.audit.info <- function(df,
                           audit_path,
                           uuid_column = "_uuid",
                           compute_edits = TRUE,
                           compute_gps = TRUE,
                           long_question_threshold = 5,
                           verbose = TRUE) {
  if (!is.data.frame(df)) stop("'df' must be a data frame")
  if (!uuid_column %in% names(df)) stop("UUID column not found in df")
  if (!dir.exists(audit_path)) stop("Audit directory does not exist")

  # Collect audit.csv files
  audit_files <- fs::dir_ls(audit_path, regexp = "audit\\.csv$", recurse = TRUE, type = "file")
  if (length(audit_files) == 0) {
    if (verbose) message("No audit.csv files found")
    df$audit_note <- "no_audit_file"
    return(df)
  }

  corrupted_files <- character(0)
  cat("\nPlease wait, it may takes some time....\n")

  # ---------------------------
  # Helper: process one file
  # ---------------------------
  process_file <- function(file) {
    uuid <- fs::path_dir(file) %>% fs::path_file()
    audit_note <- ""

    # Try read
    audit_df <- tryCatch({
      readr::read_csv(file, show_col_types = FALSE, progress = FALSE)
    }, error = function(e) {
      corrupted_files <<- c(corrupted_files, fs::path_file(file))
      return(NULL)
    })

    if (is.null(audit_df) || nrow(audit_df) == 0) {
      return(tibble::tibble(uuid = uuid, audit_note = "corrupted_or_empty"))
    }

    audit_df <- janitor::clean_names(audit_df)

    # ---------------- GPS ----------------
    gps_lat <- gps_lon <- gps_min_acc <- gps_max_acc <- gps_na <- NA_real_
    gps_max_move <- NA_real_

    if (compute_gps) {
      if (all(c("latitude", "longitude") %in% names(audit_df))) {
        df_valid <- dplyr::filter(audit_df, !is.na(latitude) & !is.na(longitude))
        if (nrow(df_valid) > 0) {
          coords <- df_valid[, c("longitude", "latitude")]
          gps_max_move <- tryCatch({
            max(geosphere::distm(coords, fun = geosphere::distHaversine))
          }, error = function(e) NA_real_)

          gps_lat <- mean(df_valid$latitude, na.rm = TRUE)
          gps_lon <- mean(df_valid$longitude, na.rm = TRUE)

          if ("accuracy" %in% names(df_valid)) {
            gps_min_acc <- suppressWarnings(min(df_valid$accuracy, na.rm = TRUE))
            gps_max_acc <- suppressWarnings(max(df_valid$accuracy, na.rm = TRUE))
          }

          if (all(c("event", "latitude", "longitude") %in% names(audit_df))) {
            gps_na <- audit_df %>%
              dplyr::filter(event %in% c("question", "group questions")) %>%
              dplyr::summarise(count = sum(is.na(latitude) | is.na(longitude))) %>%
              dplyr::pull(count)
          }
        } else {
          audit_note <- paste(audit_note, "no_valid_gps")
        }
      } else {
        audit_note <- paste(audit_note, "gps_columns_missing")
      }
    }

    # ---------------- Duration ----------------
    interview_dur <- delayed <- NA_real_
    nav_count <- 0
    form_exited <- FALSE
    audit_remarks <- NA_character_
    edit_count <- NA_integer_

    if (all(c("event", "start", "end") %in% names(audit_df))) {
      first_start <- suppressWarnings(min(audit_df$start, na.rm = TRUE))
      last_end <- suppressWarnings(max(audit_df$end, na.rm = TRUE))
      total_time <- (last_end - first_start) / (1000 * 60)

      # --- group exclusion logic ---
      group_nodes <- audit_df %>%
        dplyr::filter(event == "group questions", !is.na(node)) %>%
        dplyr::pull(node)

      exclude_patterns <- paste0("^", gsub("([\\W])", "\\\\\\1", group_nodes), "/")

      filtered <- audit_df %>%
        dplyr::filter(event %in% c("question", "group questions")) %>%
        dplyr::filter(!(event == "question" & !is.na(node) &
                          Reduce(`|`, lapply(exclude_patterns, function(p) grepl(p, node))))) %>%
        dplyr::mutate(
          duration_minutes = (end - start) / (1000 * 60),
          long_question = ifelse(duration_minutes > long_question_threshold,
                                 sub(".*/", "", node), NA_character_)
        ) %>%
        dplyr::filter(!is.na(duration_minutes), duration_minutes >= 0)
      # ----------------------------------------

      interview_dur <- suppressWarnings(sum(filtered$duration_minutes, na.rm = TRUE))
      delayed <- max(0, total_time - interview_dur, na.rm = TRUE)
      nav_count <- nrow(filtered)
      form_exited <- any(audit_df$event == "form resume", na.rm = TRUE)

      long_q <- unique(na.omit(filtered$long_question))
      if (length(long_q) > 0) {
        audit_remarks <- paste0("Long spent time more than ", long_question_threshold,
                                " minutes: ", paste(long_q, collapse = "; "))
      }
    } else {
      audit_note <- paste(audit_note, "duration_columns_missing")
    }

    # ---------------- Edits ----------------
    if (compute_edits) {
      old_col <- intersect(names(audit_df), c("old.value", "old_value", "old-value"))
      if (length(old_col) > 0) {
        edit_count <- sum(!is.na(audit_df[[old_col[1]]]) &
                            audit_df[[old_col[1]]] != "", na.rm = TRUE)
      } else {
        audit_note <- paste(audit_note, "no_edit_column")
      }
    }

    out <- tibble::tibble(
      uuid = uuid,
      audit_interview_duration = round(interview_dur, 1),
      audit_delayed_time = round(delayed, 2),
      audit_form_exited = form_exited,
      audit_number_of_navigation = nav_count,
      audit_edit_count = edit_count,
      audit_spent_too_much_time = audit_remarks,
      audit_note = trimws(audit_note)
    )

    if (compute_gps) {
      out <- dplyr::bind_cols(out, tibble::tibble(
        audit_gps_maximum_movement = round(gps_max_move, 1),
        audit_centroid_latitude = gps_lat,
        audit_centroid_longitude = gps_lon,
        audit_gps_min_accuracy = gps_min_acc,
        audit_gps_max_accuracy = gps_max_acc,
        audit_no_gps = gps_na
      ))
    }

    return(out)
  }

  # ---------------------------
  # Process all files
  # ---------------------------
  results <- purrr::map_dfr(audit_files, process_file) %>%
    dplyr::relocate(audit_note, .after = dplyr::last_col())
  # Join with df

  joint_df <- df %>% dplyr::inner_join(results, by = setNames("uuid", uuid_column))

  if (nrow(joint_df) == 0) {
    warning("No audit information added!")
    warning("Please check if relevant audit files are available and audit path is correct.")
    if (verbose) {
      cat("\n----------------- Summary -----------------\n")
      cat("Main data rows:   ", nrow(df), "\n")
      cat("Audit files found:", length(audit_files), "\n")
      cat("Corrupted files:  ", length(corrupted_files), "\n")
      if (length(corrupted_files) > 0) cat(" ->", paste(corrupted_files, collapse = ", "), "\n")
      cat("-------------------------------------------\n")
    return(df)
    }
  } else {
    final <- df %>% dplyr::left_join(results, by = setNames("uuid", uuid_column))
    if (verbose) {
      cat("\n----------------- Summary -----------------\n")
      cat("Main data rows:   ", nrow(df), "\n")
      cat("Audit files found:", length(audit_files), "\n")
      cat("Audit files added:", nrow(joint_df), "\n")
      cat("Corrupted files:  ", length(corrupted_files), "\n")
      if (length(corrupted_files) > 0) cat(" ->", paste(corrupted_files, collapse = ", "), "\n")
      cat("-------------------------------------------\n")
    }
    return(final)

  }

}
