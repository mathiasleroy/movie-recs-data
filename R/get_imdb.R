pacman::p_load(dplyr)
pacman::p_load(readr)
pacman::p_load(qs)

#' Download IMDb data
#'
#' Downloads basics + ratings, from datasets.imdbws.com, and joins them into one dataframe
#'
#' @return tibble
#' @noRd
pull_imdb <- function() {
  url_base <- "https://datasets.imdbws.com/"
  url_basics <- paste0(url_base, "title.basics.tsv.gz")
  url_ratings <- paste0(url_base, "title.ratings.tsv.gz")
  # url_akas <- paste0(url_base, "title.akas.tsv.gz")
  # url_episodes <- paste0(url_base, "title.episode.tsv.gz")
  # url_crew <- paste0(url_base, "title.crew.tsv.gz")
  # url_principals <- paste0(url_base, "title.principals.tsv.gz")
  # url_name_basics <- paste0(url_base, "name.basics.tsv.gz")

  ## DOWNLOAD ---
  df_basics <- read_tsv(url_basics, na = "\\N") ## >1GB file / Sep 2025: # A tibble: 11,926,887 × 9
  df_ratings <- read_tsv(url_ratings, na = "\\N") ## Sep 2025: # A tibble: 1,616,662 × 3
  # df_akas <- read_tsv(url_akas, na = "\\N")                 ## HUGE ! >2GB / # A tibble: 49,881,352 × 8
  # df_episodes <- read_tsv(url_episodes, na = "\\N")         ##
  # df_crew <- read_tsv(url_crew, na = "\\N")                 ## 11,929,296 × 3
  # df_principals <- read_tsv(url_principals, na = "\\N")     ##
  # df_name_basics <- read_tsv(url_name_basics, na = "\\N")   ## 14,729,546 × 6

  ## MERGE basics and ratings ---
  df_imdb <- df_basics %>% inner_join(df_ratings, by = "tconst")

  return(df_imdb)
}



#' return df_imdb_movies
#' @param force_update if FALSE, load most recent previously downloaded.
#' @param what "all", "movies", "series"
#' @noRd
load_imdb <- function(what = "all", force_update = FALSE) {
  ## WITH DATES IN FNAMES ---
  # if (force_update) {
  #   fdate <- Sys.Date()
  #   fname_movies  <- glue::glue("data/df_imdb_movies_{fdate}.qs")
  # } else {
  #   fname_movies <- (list.files("data", pattern = "df_imdb_movies_.*.qs", full.names = TRUE) |> sort(decreasing = T))[1]
  #   fdate <- as.Date(sub(".*_(\\d{4}-\\d{2}-\\d{2})\\..*", "\\1", fname_movies))
  # }
  # message("- file date = ", fdate)
  # fname_sm <- glue::glue("data/df_imdb_sm_{fdate}.qs")
  # fname_series <- glue::glue("data/df_imdb_series_{fdate}.qs")

  ## WITHOUT DATES IN FNAMES ---
  fname_movies <- "data/df_imdb_movies.qs"
  fname_series <- "data/df_imdb_series.qs"
  fname_sm <- "data/df_imdb_sm.qs"

  if (what == "all") target <- fname_sm
  if (what == "movies") target <- fname_movies
  if (what == "series") target <- fname_series

  if (!file.exists(target)) {
    df_imdb <- pull_imdb()
    # df_imdb %>% pull(titleType) %>% unique() %>% dput()
    # c("short", "movie", "tvShort", "tvEpisode", "tvSeries",
    # "tvMovie", "tvMiniSeries", "tvSpecial", "video", "videoGame")

    if (what == "all") {
      df_imdb_sm <-
        df_imdb %>%
        filter(titleType == "movie" | titleType == "tvSeries" |
          titleType == "tvMiniSeries") %>%
        filter(numVotes >= 50)
      df_imdb_sm %>% qs::qsave(fname_sm)
    } else if (what == "movies") {
      ## MOVIES ---
      df_imdb_movies <- df_imdb %>%
        filter(titleType == "movie") %>%
        # filter(numVotes >= 5) %>%
        select(-endYear, -titleType)
      df_imdb_movies %>% qs::qsave(fname_movies)
    } else if (what == "series") {
      ## SERIES + MINISERIES ---
      df_imdb_series <- df_imdb %>%
        # filter(numVotes >= 5) %>%
        filter(titleType == "tvSeries" | titleType == "tvMiniSeries")
      df_imdb_series %>% qs::qsave(fname_series)
    }
  } else {
    if (what == "all") df_imdb_sm <- qs::qread(fname_sm)
    if (what == "movies") df_imdb_movies <- qs::qread(fname_movies)
    if (what == "series") df_imdb_series <- qs::qread(fname_series)
  }

  if (what == "all") {
    message("- loaded ", nrow(df_imdb_sm), " imdb movies + series")
    # return(df_imdb_sm)
  } else if (what == "series") {
    message("- loaded ", nrow(df_imdb_series), " imdb series")
    # return(df_imdb_series)
  } else if (what == "movies") {
    message("- loaded ", nrow(df_imdb_movies), " imdb movies")
    # return(df_imdb_movies)
  }
}

# df_imdb$numVotes %>% ascii_hist(log_base = 10)
# df_imdb$numVotes %>% ascii_hist(log_base = 10, max_value = 50)


# udpate_imdb_qs <- function() {
#   load_imdb(what = "all", force_update = TRUE)
# }


## NOTES

#### IMDB NON-COMMERCIAL DATASETS -----
## 7 tables are available on https://datasets.imdbws.com/
# documentation: https://developer.imdb.com/non-commercial-datasets/
#
### title.akas.tsv.gz
# - titleId (string) - a tconst, an alphanumeric unique identifier of the title
# - ordering (integer) – a number to uniquely identify rows for a given titleId
# - title (string) – the localized title
# - region (string) - the region for this version of the title
# - language (string) - the language of the title
# - types (array) - Enumerated set of attributes for this alternative title. One or more of the following: "alternative", "dvd", "festival", "tv", "video", "working", "original", "imdbDisplay". New values may be added in the future without warning
# - attributes (array) - Additional terms to describe this alternative title, not enumerated
# - isOriginalTitle (boolean) – 0: not original title; 1: original title
#
### title.basics.tsv.gz
# - tconst (string) - alphanumeric unique identifier of the title
# - titleType (string) – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
# - primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release
# - originalTitle (string) - original title, in the original language
# - isAdult (boolean) - 0: non-adult title; 1: adult title
# - startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year
# - endYear (YYYY) – TV Series end year. '\N' for all other title types
# - runtimeMinutes – primary runtime of the title, in minutes
# - genres (string array) – includes up to three genres associated with the title
#
### title.crew.tsv.gz
# - tconst (string) - alphanumeric unique identifier of the title
# - directors (array of nconsts) - director(s) of the given title
# - writers (array of nconsts) – writer(s) of the given title
#
### title.episode.tsv.gz
# - tconst (string) - alphanumeric identifier of episode
# - parentTconst (string) - alphanumeric identifier of the parent TV Series
# - seasonNumber (integer) – season number the episode belongs to
# - episodeNumber (integer) – episode number of the tconst in the TV series
#
### title.principals.tsv.gz
# - tconst (string) - alphanumeric unique identifier of the title
# - ordering (integer) – a number to uniquely identify rows for a given titleId
# - nconst (string) - alphanumeric unique identifier of the name/person
# - category (string) - the category of job that person was in
# - job (string) - the specific job title if applicable, else '\N'
# - characters (string) - the name of the character played if applicable, else '\N'
#
### title.ratings.tsv.gz
# - tconst (string) - alphanumeric unique identifier of the title
# - averageRating – weighted average of all the individual user ratings
# - numVotes - number of votes the title has received
#
### name.basics.tsv.gz
# - nconst (string) - alphanumeric unique identifier of the name/person
# - primaryName (string)– name by which the person is most often credited
# - birthYear – in YYYY format
# - deathYear – in YYYY format if applicable, else '\N'
# - primaryProfession (array of strings)– the top-3 professions of the person
# - knownForTitles (array of tconsts) – titles the person is known for




### RANKINGS:
# https://www.imdb.com/search/title/?title_type=feature,tv_movie&user_rating=1,10&count=5




## EXPLORE AKAS ---
# {
#   # mem() # takes ~4gb memory!

#   # df_akas_counts = df_akas %>% count(titleId)
#   # df_akas_counts %>% count(n, sort=T)

#   glimpse(df_akas)
#   # $ titleId         <chr>
#   # $ ordering        <dbl>
#   # $ title           <chr>
#   # $ region          <chr>
#   # $ language        <chr>
#   # $ types           <chr>
#   # $ attributes      <chr>
#   # $ isOriginalTitle <dbl>


#   df_akas %>%
#     filter(titleId == "tt37232949") %>%
#     print()

#   df_akas %>%
#     head(1e4) %>%
#     count(region, sort = T) %>%
#     print(n = 99)

#   df_akas %>%
#     head(1e4) %>%
#     count(attributes, sort = T)

#   # df_akas_counts =
#   df_akas %>%
#     tail(1e4) %>%
#     # filter(is.na(attributes)) %>%
#     # filter(!is.na(region)) %>%
#     rename(tconst = titleId) %>%
#     summarise(
#       .by = tconst,
#       n = n(),
#       regions = paste(sort(unique(region)), collapse = ","),
#       n.regions = sum(!is.na(region)),
#       # languages = paste(sort(unique(language)), collapse = ","),
#       # n.languages = sum(!is.na(language)),
#       india = any(region == "IN", na.rm = TRUE),
#     ) %>%
#     print()
# }

# ## MERGE EVERYTHING ---
# df_basics %>%
#   filter(titleType == "movie") %>%
#   filter(runtimeMinutes > 50) %>%
#   filter(startYear > 1990) %>%
#   left_join(df_ratings, by = "tconst") %>%
#   arrange(numVotes |> desc()) %>%
#   head(10) %>%
#   left_join(df_crew, by = "tconst") %>%
#   left_join(df_name_basics, by = c("directors" = "nconst")) %>%
#   mutate(n.directors = sapply(strsplit(directors, ","), length)) %>%
#   glimpse()
