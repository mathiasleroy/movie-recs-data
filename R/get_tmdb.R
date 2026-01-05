## REQS ---
library(dplyr)
library(readr)
library(tidyr)
library(qs)
library(progress)
library(httr)
library(jsonlite)
library(purrr)


if (find("filter")[1] != "package:dplyr") stop("conflicts with dplyr")
# tmdbapikey <- Sys.getenv("TMDB_API_KEY")
# tmdbapikey <- Sys.getenv("TMDBAPIKEY")
tmdbapikey <- Sys.getenv("TMDBAPI_KEY")



#' Download TMDB data for 1 movie. Todo: handle person_results, tv_results, tv_episode_results, tv_season_results
#' @param movieID IMDb movie ID
#' @return data.frame
#' @noRd
fetch_tmdb <- function(movieID) {
  # https://api.themoviedb.org/3/find/tt0059855?api_key={tmdbapikey}&external_source=imdb_id
  url <- paste0("https://api.themoviedb.org/3/find/", movieID, "?api_key=", tmdbapikey, "&external_source=imdb_id")
  response <- tryCatch(GET(url), error = function(e) NULL)

  if (!is.null(response) && http_status(response)$category == "Success") {
    parsed_json <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    # names(parsed_json) = c("movie_results", "person_results", "tv_results", "tv_episode_results", "tv_season_results")

    # length(parsed_json$movie_results)>1
    # length(parsed_json$tv_results)>1

    if (length(parsed_json$movie_results) > 0) {
      res <- parsed_json$movie_results[1, ] %>% mutate(type = "movie")
    } else if (length(parsed_json$tv_results) > 0) {
      res <- parsed_json$tv_results[1, ] %>% mutate(type = "tv")
    } else {
      res <- NULL
    }

    ## movie:
    # c("adult",
    #   "backdrop_path",
    #   "id",                       /
    #   "title",                    /
    #   "original_title",           /
    #   "overview",               v
    #   "poster_path",            v
    #   "media_type",               /
    #   "original_language",      v
    #   "genre_ids",                /
    #   "popularity",             v
    #   "release_date",           v
    #   "video",                    /
    #   "vote_average",             /
    #   "vote_count")               /
    ## TV
    # names(res) |> dput()
    # c("adult",                  /
    #   "backdrop_path",          /
    #   "id",                     /
    #   "name",                   /
    #   "original_name",         v
    #   "overview",              v
    #   "poster_path",           v
    #   "media_type",             /
    #   "original_language",     v
    #   "genre_ids",              /
    #   "popularity",            v
    #   "first_air_date",        v
    #   "vote_average",           /
    #   "vote_count",             /
    #   "origin_country")        v

    if (!is.null(res) && nrow(res) >= 1) {
      if ("release_date" %in% names(res)) {
        res <- res %>% rename(date_release = release_date)
      }
      if ("first_air_date" %in% names(res)) {
        res <- res %>% rename(date_release = first_air_date)
      }
      if ("date_release" %in% names(res)) {
        res <- res %>%
          mutate(
            date_release = if (!is.null(res$date_release)) res$date_release %>% as.Date(format = "%Y-%m-%d") else NA
          )
      }

      if ("id" %in% names(res)) {
        res <- res %>% rename(tmdbid = id)
      }
      if ("original_name" %in% names(res)) {
        res <- res %>% rename(original_title = original_name)
      }
      # data.frame(
      #   tmdbid = if (!is.null(res$id)) res$id else NA,
      #   original_title = if (!is.null(res$original_title)) res$original_title else NA,
      #   date_release = if (!is.null(res$release_date)) res$release_date %>% as.Date(format = "%Y-%m-%d") else NA,
      #   popularity = if (!is.null(res$popularity)) res$popularity else NA,
      #   original_language = if (!is.null(res$original_language)) res$original_language else NA,
      #   overview = if (!is.null(res$overview)) res$overview else NA,
      #   poster_path = if (!is.null(res$poster_path)) res$poster_path else NA,
      #   # origin_country = res$origin_country,
      #   # date_fetched = Sys.Date()
      #   type = "movie"
      # )
      # data.frame(
      #   tmdbid = if (!is.null(res$id)) res$id else NA,
      #   original_title = res$original_name, ## diff
      #   date_release = res$first_air_date %>% as.Date(format = "%Y-%m-%d"), ## diff
      #   popularity = res$popularity,
      #   original_language = res$original_language,
      #   overview = res$overview,
      #   poster_path = res$poster_path,
      #   # origin_country = res$origin_country, ## not in movie
      #   # date_fetched = Sys.Date()
      #   type = "tv"
      # )
    }
  }
  ## Return NA if no movie results are found or API request failed
  return(res)
}
## examples:
# fetch_tmdb("tt21382296") %>% glimpse()
# fetch_tmdb("tt0059855") %>% glimpse() ## chinese
# fetch_tmdb("tt27426266") %>% glimpse() ## NULL
# fetch_tmdb("tt37232949") %>% glimpse() ## Paderu 12Mile : no poster, no release_date
# fetch_tmdb("tt24074470") %>% glimpse() ##
# fetch_tmdb("tt0944947") %>% glimpse() ## TV: game of thrones

movieID <- "tt0092337"
# fetch_tmdb("tt0092337") %>% glimpse() ## both $movie_results + $tv_results / its a tv-mini-series: https://www.imdb.com/title/tt0092337/
# -> wrongly classified as a movie !!

# bind_rows(
#   fetch_tmdb("tt21382296"),
#   fetch_tmdb("tt0059855"),
#   fetch_tmdb("tt27426266"), ## wont appear
#   fetch_tmdb("tt37232949"),
#   fetch_tmdb("tt24074470"),
#   fetch_tmdb("tt0944947")
# ) %>% tibble()




#' Fetch tdmdb data one by one from the imdb api.
#'
#' This task will run continuously, and can be interrupted with ctrl-c.
#' The qs file is incrementally updated with new data and saved after every batch (of batch_size)
#'
#' @param batch_size number of movies to fetch per batch
#' @param do_redo_na redo movies with NA values. Skips them if FALSE.
#' @param date_refetch refetch movies older than this, leave very low to not refetch
#' @return NULL, saves to data/df_tmdb.qs
#' @noRd
batch_scrape_tmdb_api <- function(
    df_tofetch,
    batch_size = 100,
    do_redo_na = FALSE,
    date_refetch = Sys.Date() - 365999,
    limit_to_fetch = 999999
    #
    ) {
  # message("\n\n=== START INFINTE FETCHING TMDB ===\n")
  # message("you can ctrl-C at any time, the qs file is saved after every batch_size\n")
  message("- batch_size: ", batch_size)

  ## IMPORT PREVIOUS RUNS
  if (file.exists("data/df_tmdb.qs")) {
    df_tmdb <-
      qs::qread("data/df_tmdb.qs") %>%
      # mutate(date_fetched = Sys.Date()) %>% ## this was abug, should not ahve been there.
      select(tconst, type, date_release, popularity, original_language, overview, poster_path, date_fetched) %>%
      distinct(tconst, .keep_all = TRUE) ## deduplicate

    ## remove data to force refetch again
    if (do_redo_na) df_tmdb <- df_tmdb %>% filter(!is.na(date_release))
    ## or:
    # df_tmdb <- df_tmdb %>% filter(
    #   !(is.na(overview) & is.na(release_date) & is.na(popularity) & is.na(original_language) & is.na(overview) & is.na(poster_path))
    # )

    df_tmdb <- df_tmdb %>% filter(date_fetched > date_refetch)
  } else {
    df_tmdb <- tibble(tconst = character())
  }

  ## LIST NOT FETCHED YET
  df_left <- df_tofetch %>%
    distinct(tconst, .keep_all = TRUE) %>%
    anti_join(df_tmdb, by = "tconst")
  df_left <- df_left %>% slice(1:limit_to_fetch)
  if (nrow(df_left) == 0) {
    return("No movies left to fetch")
  }

  ## COUNTS
  n_fetched <- nrow(df_tmdb)
  n_lefttofetch <- nrow(df_left)
  n_batches <- ceiling(n_lefttofetch / batch_size)
  message("- ", nrow(df_tmdb), " movies already fetched (", round(100 * nrow(df_tmdb) / nrow(df_tofetch)), "%)")
  message("- ", nrow(df_left), " movies left to fetch", " (", n_batches, " batches)")

  ## INFO
  if ("numVotes" %in% names(df_left)) message("- current max numVotes = ", max(df_left$numVotes))
  if ("rating" %in% names(df_left)) message("- current max rating = ", max(df_left$rating))

  progress_bar <- progress::progress_bar$new(
    total = nrow(df_left),
    format = "(:bar) :percent [:elapsed/:eta] :current/:total (:tick_rate/s)"
  )

  ### LOOP TO ITERATE MORE AND MORE INFO -----
  start_index <- 1
  iii <- 1
  for (iii in start_index:n_batches) {
    start <- (iii - 1) * batch_size + 1
    end <- min(iii * batch_size, n_lefttofetch)

    df_newbatch <- df_left %>%
      dplyr::slice(start:end) %>%
      # print(n = 100)
      mutate(tmdb_data = purrr::map(tconst, ~ {
        result <- fetch_tmdb(.x)
        progress_bar$tick()
        return(result)
      })) %>%
      unnest_wider(tmdb_data) %>%
      select(tconst, date_release, popularity, original_language, overview, poster_path, type) %>%
      # # mutate(release_date = as.Date(release_date, format = "%Y-%m-%d")) %>%
      mutate(date_fetched = Sys.Date())
    # df_newbatch %>% print(n = 100)

    if (exists("df_tmdb")) {
      df_tmdb <- bind_rows(df_tmdb, df_newbatch)
    } else {
      df_tmdb <- df_newbatch
    }


    df_tmdb %>% qs::qsave("data/df_tmdb.qs")
    ## backup
    # r <- file.copy("data/df_tmdb.qs", "data/df_tmdb_backup.qs", overwrite = T) ## in case of corruption
    # if (!isTRUE(r)) stop("backup failed")
  } ## this loop might never finish, can safely be interrupted with ctrl-c
}
## usage:
# df_tofetch <- data.frame(tconst = c("tt21382296", "tt0059855", "tt27426266", "tt37232949", "tt24074470", "tt0944947"))
# df_tofetch %>% batch_scrape_tmdb_api(limit_to_fetch = 100)
df_tofetch %>% batch_scrape_tmdb_api()


# ## make manual changes to df_tmdb
if (0) {
  df_tmdb <- qs::qread("data/df_tmdb.qs")

  # # df_tmdb <- df_tmdb %>% mutate(type = coalesce(type, "movie"))
  df_tmdb <-
    df_tmdb %>%
    left_join(df_imdb_sm %>% select(tconst, titleType), by = "tconst") %>%
    mutate(type = recode(titleType, "movie" = "movie", "tvSeries" = "tv", "tvMiniSeries" = "tv")) %>%
    select(-titleType)

  df_tmdb %>% count(type)
  # df_tmdb <- df_tmdb %>% mutate(date_release = coalesce(date_release, release_date))
  # df_tmdb %>% count(date_release, sort = T)
  # df_tmdb <- df_tmdb %>% select(-release_date)
  df_tmdb %>% qs::qsave("data/df_tmdb.qs")
}







check_tmdb <- function() {
  df_tmdb <- qs::qread("data/df_tmdb.qs")
  df_tmdb %>% arrange(popularity |> desc())

  message("- loaded ", nrow(df_tmdb), " tmdb movies")
  df_tmdb %>%
    count(type, sort = T) %>%
    print()
  df_tmdb$date_release %>% range(na.rm = T)
}
## usage:
# check_tmdb()
