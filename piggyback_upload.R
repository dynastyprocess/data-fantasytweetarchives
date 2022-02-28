library(tidyverse)
x <- list.files("data",recursive = TRUE,full.names = TRUE) |> 
  purrr::map_dfr(~readr::read_csv(.x) |> suppressMessages()) |> 
  dplyr::mutate(across(contains("id"),as.character))

x |> 
  distinct(status_id,.keep_all = TRUE) |> 
  write.csv("tweets_32beatwriters.csv")

piggyback::pb_new_release(tag = "32beatwriters", body = paste0("Tweets from @32BeatWriters, last updated: ",Sys.time()))

piggyback::pb_upload("tweets_32beatwriters.csv",tag = "32beatwriters")
