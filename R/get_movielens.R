#' returns df_movielens if not exists
#' if necessary, Download Movielens zip files. then unzip and joins ratings and links
#'
#' @return tibble ; select(userId, imdbId, rating)
#' @noRd
load_movielens <- function(force_update = FALSE) {
  # https://files.grouplens.org/datasets/movielens/ml-32m-README.html
  urlzip <- "https://files.grouplens.org/datasets/movielens/ml-32m.zip"
  urlmd5 <- "https://files.grouplens.org/datasets/movielens/ml-32m.zip.md5"
  expected_md5 <- strsplit(readLines(urlmd5), "  ")[[1]][1] ## changes if the file changes
  zip_path <- "data/ml-32m.zip"

  ## if the qs file exists, and no force update, use that.
  ## if doesnt exist, or forced update, we need the zip file to know the md5
  ## if md5 is same we dont need to redowload the zip file.

  fname_qs <- "data/df_movielens.qs"
  if (file.exists(fname_qs) && !force_update) {
    ## the file didnt change since last time,
    message("- use last QS file")
    df_movielens <- qs::qread(fname_qs)
  } else {
    message("Do not load QS file")
    ## DOWNLOAD ZIP ---
    ## if the zip file is not present we cant check its md5, therefore we need to download it.
    current_md5 <- if (file.exists(zip_path)) tools::md5sum(zip_path) else ""
    if (current_md5 != expected_md5) {
      message("Download zip file")
      download.file(urlzip, zip_path, mode = "wb")
      current_md5 <- tools::md5sum(zip_path)
    } else {
      message("Zip file is up to date")
    }

    ## todo:
    #  maye the zip was already unzipped and we dont need to unzip again.

    ## UNZIP ---
    folder <- "data/ml-32m"
    if (!dir.exists(folder)) dir.create(folder)
    if (!dir.exists(folder)) {
      unzip(zip_path, exdir = "data")
    }
    ## csv checksums
    # md5.files <- readLines(paste0(folder, "/checksums.txt")) |> strsplit("  ")
    # files <- c("links.csv", "movies.csv", "ratings.csv", "tags.csv")
    # for (fff in files) {
    #   expected <- md5.files[[match(fff, sapply(md5.files, `[`, 2))]][1]
    #   if (tools::md5sum(file.path(folder, fff)) != expected) abort("MD5 mismatch: ", fff)
    # }

    if (!exists("df_movielens")) {
      ml.links <- readr::read_csv(paste0(folder, "/links.csv"), show_col_types = F, progress = F)
      ml.ratings <- readr::read_csv(paste0(folder, "/ratings.csv"), show_col_types = F, progress = F)
      # ml.movies <- ...
      # ml.tags <- ...

      ## join
      df_movielens <- ml.ratings %>%
        left_join(ml.links, by = "movieId") %>%
        select(userId, imdbId, rating)
    }


    ## SUBSET ---
    {
      ## df_movielens is too large
      # df_movielens %>% count(userId) %>% arrange(n) # min is 20
      # df_movielens %>% count(imdbId) %>% arrange(n) # min is 1
      min_n <- 50
      df_ml_sm <- df_movielens
      for (i in 1:3) { ## 3 passes
        df_ml_sm <- df_ml_sm %>% filter(.by = imdbId, n() >= min_n)
        df_ml_sm <- df_ml_sm %>% filter(.by = userId, n() >= min_n)
      }
      # df_ml_sm %>% count(imdbId) %>% arrange(n)
      # df_ml_sm %>% count(userId) %>% arrange(n)

      df_movielens <- df_ml_sm
    }

    df_movielens %>% qs::qsave(fname_qs)
    message("- saved ", nrow(df_movielens), " movielens ratings in ", fname_qs)
    # df_ml_sm %>% qs::qsave("data/df_ml_sm.qs") ## 78MB
  }

  # message("- loaded ", nrow(df_movielens), " movielens ratings")
  # return(df_movielens)
}
# df_movielens <- load_movielens()

# > glimpse(ml.links, 0)
# Rows: 87,585
# Columns: 3
# $ movieId <dbl> …
# $ imdbId  <chr> …
# $ tmdbId  <dbl> …
# > glimpse(ml.movies, 0)
# Rows: 87,585
# Columns: 3
# $ movieId <dbl> …
# $ title   <chr> …
# $ genres  <chr> …
# > glimpse(ml.ratings, 0)
# Rows: 32,000,204
# Columns: 4
# $ userId    <dbl> …
# $ movieId   <dbl> …
# $ rating    <dbl> …
# $ timestamp <dbl> …
# > glimpse(ml.tags, 0)
# Rows: 2,000,072
# Columns: 4
# $ userId    <dbl> …
# $ movieId   <dbl> …
# $ tag       <chr> …
# $ timestamp <dbl> …
