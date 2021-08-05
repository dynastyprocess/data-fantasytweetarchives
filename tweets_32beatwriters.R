library(rtweet)
library(arrow)
library(dplyr)
library(purrr)

twitter_token <- rtweet::create_token(
  app = "Tan-DataCollection",
  consumer_key = Sys.getenv("TwitterAPIKey"),
  consumer_secret = Sys.getenv("TwitterAPISecret"),
  access_token = Sys.getenv("TwitterAccessToken"),
  access_secret = Sys.getenv("TwitterAccessTokenSecret")
)

min_id <- arrow::open_dataset("data/32beatwriters", format = "csv") %>% 
  select(status_id) %>% 
  collect() %>% 
  filter(status_id == max(status_id)) %>% 
  pull()

tweets_32 <- rtweet::get_timelines(user = "32beatwriters", 
                                   n = 3200,
                                   token = twitter_token) %>% 
  filter(status_id >= min_id) %>% 
  mutate(
    year = format(created_at, "%Y"),
    month = format(created_at, "%m"),
    across(where(is.list),~paste(collapse = "; ")),
    across(contains("created_at"), as.character)
  )

arrow::write_dataset(tweets_32,
                     path = "data/32beatwriters",
                     format = "csv",
                     partitioning = c("year","month"))
