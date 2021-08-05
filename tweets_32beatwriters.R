library(rtweet)
library(arrow)
library(dplyr)
library(purrr)

twitter_token <- rtweet::create_token(
  app = "Tan-DataCollection",
  consumer_key = Sys.getenv("TWITTERAPIKEY"),
  consumer_secret = Sys.getenv("TWITTERAPISECRET"),
  access_token = Sys.getenv("TWITTERACCESSTOKEN"),
  access_secret = Sys.getenv("TWITTERACCESSTOKENSECRET")
)

min_id <- arrow::open_dataset("data/32beatwriters", format = "csv") %>% 
  select(status_id) %>% 
  collect() %>% 
  filter(status_id == max(status_id)) %>% 
  pull(status_id)

tweets_32 <- rtweet::get_timeline(user = "32beatwriters", 
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
