library(dplyr)
library(readr)
library(qs)
library(glue)
library(tools)

# setwd(dirname(rstudioapi::getSourceEditorContext()$path))
# getwd()

if (!dir.exists("data")) dir.create("data")

### LOAD FUNCTIONS -----
source("R/get_imdb.R")
source("R/get_movielens.R")
source("R/get_tmdb.R")


### CREATE QS FILES -----

message("## 1. data/df_imdb_sm_{date}.qs ---")
load_imdb(what = "all", force_update = TRUE)

message("## 2. data/df_movielens.qs ---")
load_movielens()

message("## 3. data/df_tmdb.qs ---")
do_use <- "imdb"
do_reverse <- 0
maxtofetch <- 200
if (do_use == "recs" & file.exists("data/df_recs.qs")) {
    message("- use recs")
    df_tofetch <- qs::qread("data/df_recs.qs")
    if (do_reverse) df_tofetch <- df_tofetch %>% arrange(rating)
} else {
    message("- use imdb")
    if (!exists("df_imdb_sm")) df_imdb_sm <- qs::qread("data/df_imdb_sm.qs")
    df_tofetch <- df_imdb_sm %>%
        arrange(numVotes |> desc()) %>%
        select(tconst, numVotes)
    if (do_reverse) df_tofetch <- df_tofetch %>% arrange(numVotes)
}
batch_scrape_tmdb_api(df_tofetch, limit_to_fetch = maxtofetch)
