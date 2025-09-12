# ------------------------------------------------------------------------------
# download.data()
# Downloads Kobo form data using a given asset_id and user credentials.
# Returns a cleaned dataframe of all sheets or NULL if download fails.
# Inputs:
# - params (list):
# * asset_id : Kobo form UID (required)
# * project_name : Name for the downloaded file (required)
# * data_folder : Folder to save file, default "./"
# - username : Kobo account username (required)
# - password : Kobo account password (required)
# - kobo_url : Base URL for Kobo API, default:
#  "https://kobo.impact-initiatives.org/api/v2"
# 
# Output:
#   - A dataframe containing the Kobo form data
# - NULL if download fails or asset not found
# ------------------------------------------------------------------------------

download.data <- function(params, username, password, kobo_url = "https://kobo.impact-initiatives.org/api/v2") {
  
  if (!require(httr, quietly = TRUE)) stop("Please install 'httr'")
  if (!require(jsonlite, quietly = TRUE)) stop("Please install 'jsonlite'")
  
  required_params <- c("asset_id", "project_name")
  missing_params <- setdiff(required_params, names(params))
  if (length(missing_params) > 0) {
    stop("Missing required params: ", paste(missing_params, collapse = ", "))
  }
  
  if (is.null(username) || username == "") stop("Username cannot be NULL or empty.")
  if (is.null(password) || password == "") stop("Password cannot be NULL or empty.")
  if (is.null(kobo_url) || kobo_url == "") stop("base_url cannot be NULL or empty.")
  
  # default values
  defaults <- list(
    group_seperator = ".",
    data_folder   = "./"
  )
  
  final_params <- modifyList(defaults, params)
  final_params$base_url = kobo_url
  
  # show start info
  cat("Please wait, it may take some time...\n")
  cat("Getting asset list...\n")
  
  # request asset list
  assets_response <- GET(
    paste0(final_params$base_url, "/assets/"),
    authenticate(username, password)
  )
  
  if (http_error(assets_response)) {
    stop("Authentication failed or server unreachable:\n", content(assets_response, "text"))
  }
  
  assets <- content(assets_response, "parsed")$results
  if (length(assets) == 0) {
    warning("No assets found for this user. Nothing to download.")
    return(NULL)
  }
  
  # check if asset exists
  asset_ids <- sapply(assets, function(x) x$uid)
  if (!(final_params$asset_id %in% asset_ids)) {
    available <- paste0(
      "  - ", sapply(assets, function(x) paste0(x$name, " (", x$uid, ")")),
      collapse = "\n"
    )
    warning(paste0(
      "The provided asset_id '", final_params$asset_id, "' was not found.\n\n",
      "Here are your available projects:\n", available
    ))
    return(NULL)
  }
  
  cat("Found asset ID:", final_params$asset_id, "\n")
  
  # create export
  cat("Creating XLSX export...\n")
  export_response <- POST(
    paste0(final_params$base_url, "/assets/", final_params$asset_id, "/exports/"),
    authenticate(username, password),
    body = list(
      type = "xls",
      fields_from_all_versions = TRUE,
      hierarchy_in_labels = FALSE,   
      group_sep = final_params$group_seperator,               
      lang = "_xml",
      include_media_url = TRUE,
      multiple_select = "both"
    ),
    encode = "json"
  )
  
  if (http_error(export_response)) {
    stop("Export creation failed: ", content(export_response, "text"))
  }
  
  export_data <- content(export_response, "parsed")
  export_uid <- export_data$uid
  
  # retry loop for download
  for (i in 1:30) {
    status_response <- GET(
      paste0(final_params$base_url, "/assets/", final_params$asset_id, "/exports/", export_uid, "/"),
      authenticate(username, password)
    )
    
    status_data <- content(status_response, "parsed")
    cat("Status:", status_data$status, "\n")
    
    if (status_data$status == "complete") {
      
      # Ensure data folder exists
      if (!dir.exists(final_params$data_folder)) {
        dir.create(final_params$data_folder, recursive = TRUE)
      }
      
      filename <- paste0(final_params$data_folder, final_params$project_name, "_", 
                         format(Sys.time(), "%Y-%m-%d__T%H-%M"), ".xlsx")
      
      download_file <- GET(
        status_data$result,
        authenticate(username, password),
        write_disk(filename, overwrite = TRUE)
      )
      
      # Get all sheet names of downloaded file
      sheets <- openxlsx::getSheetNames(filename)
      
      cleaned_sheets <- lapply(sheets, function(sheet) {
        df <- read.xlsx(filename, sheet = sheet)
        
        if (nrow(df) == 0) return(df)  # skip empty sheets
        
        df <- df %>%
          mutate(across(where(~ all(grepl("^-?[0-9.]+$", .x) | is.na(.x))), as.numeric)) %>%
          mutate(across(where(is.character),
                        ~ str_replace_all(.x, "xml:space=\"preserve\">", "") %>%
                          str_replace_all("</.*?>", "") %>%
                          str_trim()))
        
        df
      })
      
      # Write back into the same file
      openxlsx::write.xlsx(cleaned_sheets, file = filename, sheetName = sheets, overwrite = TRUE)
      
      
      if (download_file$status_code == 200) {
        cat("\n✅ Download completed:", filename, "\n")
        df <- readxl::read_excel(filename)
        cat("Total records: ", nrow(df), "\n")
        return(df)
      } else {
        stop("Download failed with status: ", download_file$status_code)
      }
      
    } else if (status_data$status == "error") {
      stop("Export error: ", ifelse(!is.null(status_data$messages), status_data$messages, "Unknown error"))
    }
    
    Sys.sleep(3)
  }
  
  warning("Export timed out after 90 seconds. No file downloaded.")
  return(NULL)
}


# ------------------------------------------------------------------------------
# download.audit()
# Downloads audit CSV files from provided URLs
# Skips existing files and returns paths of all downloaded or existing audits.
# ------------------------------------------------------------------------------

download.audit <- function(params, username, password, workers = 5, batch_size = 50) {
  if (!requireNamespace("httr", quietly = TRUE)) {
    stop("Please install 'httr' package: install.packages('httr')")
  }
  if (!requireNamespace("future.apply", quietly = TRUE)) {
    stop("Please install 'future.apply' package: install.packages('future.apply')")
  }
  
  `%||%` <- function(a, b) if (!is.null(a)) a else b
  
  required_params <- c("df", "audit_folder")
  missing_params <- setdiff(required_params, names(params))
  if (length(missing_params) > 0) {
    stop("Missing required parameter(s): ", paste(missing_params, collapse = ", "))
  }
  
  if (is.null(username) || username == "") stop("❌ Username cannot be NULL or empty.")
  if (is.null(password) || password == "") stop("❌ Password cannot be NULL or empty.")
  

  df <- params$df
  audit_folder <- params$audit_folder
  url_column <- params$url_column  %||% "audit_URL"
  uuid_column <- params$uuid_column %||% "_uuid"
  
  if (!all(c(url_column, uuid_column) %in% colnames(df))) {
    stop("Dataframe is missing required column(s): ",
         paste(setdiff(c(url_column, uuid_column), colnames(df)), collapse = ", "))
  }
  
  urls  <- df[[url_column]]
  uuids <- df[[uuid_column]]
  
  if (nrow(df) == 0) {
    warning("Dataframe has no rows. Nothing to download.")
    return(character(0))
  }
  if (all(is.na(urls))) {
    warning("No valid URLs found in column '", url_column, "'. Nothing to download.")
    return(character(0))
  }
  
  if (!dir.exists(audit_folder)) dir.create(audit_folder, recursive = TRUE)
  
  # skip existing files
  existing_files <- list.files(audit_folder, pattern = "audit.csv$", recursive = TRUE, full.names = TRUE)
  existing_uuids <- basename(dirname(existing_files))
  mask <- !(uuids %in% existing_uuids)
  df_to_download <- df[mask, , drop = FALSE]
  
  if (nrow(df_to_download) == 0) {
    cat("✅ All audit.csv files already exist. Nothing to download.\n")
    return(invisible(existing_files))
  }
  
  # worker function
  download_file <- function(url, uuid, folder, username, password) {
    if (is.na(url) || is.na(uuid)) return(NULL)
    folder_path <- file.path(folder, uuid)
    if (!dir.exists(folder_path)) dir.create(folder_path, recursive = TRUE)
    filepath <- file.path(folder_path, "audit.csv")
    
    tryCatch({
      res <- httr::GET(url, httr::authenticate(username, password))
      if (httr::status_code(res) == 200) {
        writeBin(httr::content(res, "raw"), filepath)
        filepath
      } else {
        message(sprintf("❌ Failed %s: HTTP %s", url, httr::status_code(res)))
        NULL
      }
    }, error = function(e) {
      message(sprintf("❌ Error downloading %s: %s", url, e$message))
      NULL
    })
  }
  
  # monitoring function
  monitor_files <- function(folder) {
    list.files(folder, pattern = "audit.csv$", recursive = TRUE, full.names = TRUE)
  }
  
  # batch processing
  total_batches <- ceiling(nrow(df_to_download) / batch_size)
  
  cat(sprintf("Existing audit files: %d \n", length(existing_files)))
  cat(sprintf("Downloading %d files...\n", nrow(df_to_download)))
  

  if (workers > 10){
    workers = 10
    cat("\nMax worker is limitted to 10!\n")
  }
  
  future::plan(future::multisession, workers = workers)
  
  all_downloaded_files <- list()
  
  for (batch in 1:total_batches) {
    start_idx <- (batch - 1) * batch_size + 1
    end_idx <- min(batch * batch_size, nrow(df_to_download))
    
    batch_df <- df_to_download[start_idx:end_idx, ]
    
    downloaded_files <- future.apply::future_mapply(
      FUN = download_file,
      url  = batch_df[[url_column]],
      uuid = batch_df[[uuid_column]],
      MoreArgs = list(folder = audit_folder, username = username, password = password),
      SIMPLIFY = FALSE
    )
    
    downloaded_files <- Filter(Negate(is.null), downloaded_files)
    all_downloaded_files <- c(all_downloaded_files, downloaded_files)
    
    # Report progress after each batch
    current_count <- length(monitor_files(audit_folder))
    cat(sprintf("Total audit files: %d\n", current_count))
  }
  
  
  # Final summary
  final_count <- length(monitor_files(audit_folder))
  cat("\n✅ Download audit finished!\n")
  cat(sprintf("Total audit files: %d\n", final_count))
  cat(sprintf("Newly downloaded: %d files\n", length(all_downloaded_files)))

  
  invisible(c(existing_files, unlist(all_downloaded_files)))
}