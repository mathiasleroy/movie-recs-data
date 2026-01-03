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
#' @param movieID IMDb ID
#' @return tibble
#' @noRd
fetch_tmdb <- function(movieID) {
  # https://api.themoviedb.org/3/find/tt0059855?api_key={tmdbapikey}&external_source=imdb_id
  url <- paste0("https://api.themoviedb.org/3/find/", movieID, "?api_key=", tmdbapikey, "&external_source=imdb_id")
  response <- tryCatch(GET(url), error = function(e) NULL)
  if (!is.null(response) && http_status(response)$category == "Success") {
    parsed_json <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
    # names(parsed_results) = c("movie_results", "person_results", "tv_results", "tv_episode_results", "tv_season_results")
    if (length(parsed_json$movie_results) > 0) {
      # c("adult", "backdrop_path", "id", "title", "original_title", "overview", "poster_path", "media_type", "original_language", "genre_ids", "popularity", "release_date", "video", "vote_average", "vote_count")
      movie_result <- parsed_json$movie_results[1, ]
      return(data.frame(
        release_date = ifelse(length(parsed_json$movie_results$release_date) >= 1, parsed_json$movie_results$release_date[1], NA),
        popularity = ifelse(length(parsed_json$movie_results$popularity) >= 1, parsed_json$movie_results$popularity[1], NA),
        original_language = ifelse(length(parsed_json$movie_results$original_language) >= 1, parsed_json$movie_results$original_language[1], NA),
        overview = ifelse(length(parsed_json$movie_results$overview) >= 1, parsed_json$movie_results$overview[1], NA),
        poster_path = ifelse(length(parsed_json$movie_results$poster_path) >= 1, parsed_json$movie_results$poster_path[1], NA)
      ))
    } else if (length(parsed_json$tv_results) > 0) {
      ## tv_results
    } else if (length(parsed_json$person_results) > 0) {
      ## person_results
    } else if (length(parsed_json$tv_season_results) > 0) {
      ## tv_season_results
    } else if (length(parsed_json$tv_episode_results) > 0) {
      ## tv_episode_results
    }
  }
  ## Return NA if no movie results are found or API request failed
  return(data.frame(
    release_date = NA,
    popularity = NA,
    original_language = NA,
    overview = NA,
    poster_path = NA
  ))
}
## examples:
# fetch_tmdb("tt21382296") %>% t() %>% tibble()
# fetch_tmdb("tt0059855") %>% t() %>% tibble()
# fetch_tmdb("tt0059855") ## no release_date
# fetch_tmdb("tt27426266") ## nothing
# fetch_tmdb("tt37232949") ## Paderu 12Mile
# fetch_tmdb("tt24074470") ##
# parsed_json$movie_results %>%
# as.list()
# names() |>
# dput()



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
    limit_to_fetch = Inf
    #
    ) {
  # message("\n\n=== START INFINTE FETCHING TMDB ===\n")
  # message("you can ctrl-C at any time, the qs file is saved after every batch_size\n")
  message("- batch_size: ", batch_size)




  ### LOOP TO ITERATE MORE AND MORE INFO -----
  {
    ## IMPORT PREVIOUS RUNS
    if (file.exists("data/df_tmdb.qs")) {
      df_tmdb <-
        qs::qread("data/df_tmdb.qs") %>%
        # mutate(date_fetched = Sys.Date()) %>% ## this was abug, should not ahve been there.
        select(tconst, release_date, popularity, original_language, overview, poster_path, date_fetched) %>%
        distinct(tconst, .keep_all = TRUE) ## deduplicate

      ## remove data to force refetch again
      if (do_redo_na) df_tmdb <- df_tmdb %>% filter(!is.na(release_date))
      df_tmdb <- df_tmdb %>% filter(date_fetched > date_refetch)
    } else {
      df_tmdb <- tibble(tconst = character())
    }



    df_left <- df_tofetch %>%
      distinct(tconst, .keep_all = TRUE) %>%
      anti_join(df_tmdb, by = "tconst")

    df_left <- df_left %>% slice(1:limit_to_fetch)

    if (nrow(df_left) == 0) {
      return("No movies left to fetch")
    }



    n_fetched <- nrow(df_tmdb)
    n_movies <- nrow(df_left)
    n_batches <- ceiling(n_movies / batch_size)

    message("- ", nrow(df_tmdb), " movies already fetched (", round(100 * nrow(df_tmdb) / nrow(df_tofetch)), "%)")
    message("- ", nrow(df_left), " movies left to fetch")


    if ("numVotes" %in% names(df_left)) message("- current max numVotes = ", max(df_left$numVotes))
    if ("rating" %in% names(df_left)) message("- current max rating = ", max(df_left$rating))

    progress_bar <- progress::progress_bar$new(
      total = nrow(df_left),
      format = "(:bar) :percent [:elapsed/:eta] :current/:total (:tick_rate/s)"
    )

    start_index <- 1
    for (i in start_index:n_batches) {
      start <- (i - 1) * batch_size + 1
      end <- min(i * batch_size, n_movies)

      df_batch <- df_left %>%
        dplyr::slice(start:end) %>%
        mutate(tmdb_data = purrr::map(tconst, ~ {
          # print(.x)
          result <- fetch_tmdb(.x)
          # print(nrow(result))
          progress_bar$tick()
          return(result)
        })) %>%
        unnest_wider(tmdb_data) %>%
        select(tconst, release_date, popularity, original_language, overview, poster_path) %>%
        mutate(release_date = as.Date(release_date, format = "%Y-%m-%d")) %>%
        mutate(date_fetched = Sys.Date())

      if (exists("df_tmdb")) {
        df_tmdb <- bind_rows(df_tmdb, df_batch)
      } else {
        df_tmdb <- df_batch
      }

      df_tmdb %>% qs::qsave("data/df_tmdb.qs")
      # r <- file.copy("data/df_tmdb.qs", "data/df_tmdb_backup.qs", overwrite = T) ## in case of corruption
      # if (!isTRUE(r)) stop("backup failed")
    }
  } ## this loop will never finish, the list is too long. the folowwing will never run:



  # glimpse(df_tmdb)
}


load_tmdb <- function() {
  df_tmdb <- qs::qread("data/df_tmdb.qs")
  message("- loaded ", nrow(df_tmdb), " tmdb movies")
  return(df_tmdb)
}

## usage:
# batch_scrape_tmdb_api() ## --> data/df_tmdb.qs
# df_tmdb <- load_tmdb()
