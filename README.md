# Prerequisite
+ Java 8
+ MongoDB

# Quickstart

> git clone https://github.com/news-document-pipeline-htw-berlin/Analytics \
> cd Analytics\
> Edit inputUri and outputUri in App\
> Make sure input MongoDB is structured according to cheat sheet \
> sbt run

# MongoDB Cheat sheet

| 0    |    _id  |
| ---- | ---- |
| 1    |  authors    |
| 2    |  crawl_time    |
| 3    |  description    |
| 4    |  entities    |
| 5    |  imgae_links    |
| 6    |  intro    |
| 7    |  keywords    |
| 8    |  keywords_extracted    |
| 9    |  lemmatizer    |
| 10    |  links    |
| 11    |  long_url    |
| 12    |  news_site    |
| 13    |  published_time   |
| 14    |  read_time    |
| 15    |  sentiments    |
| 16    |  short_url    |
| 17    |  text    |
| 18    |  title    |

+ _id                 = unique Hash to identify a single article 
+ authors             = an array containing the authors
+ crawl_time          = time-spamp informing when the article was crawled
+ description         = ???
+ entities            = named entities and their predicted category (PERson, LOCation, ORGanisation, MISCellaneous)
+ image_links         = an array, containing the links of the images used in the article
+ intro               = introduction text 
+ keywords            = an array, containing keywords given by the author
+ keywords_extracted  = an array, containing keywords extracted by the analytics team
+ lemmatizer          = content of "StopWordCleaner", but tokens are reduced to their root/neutral form
+ links               = an array, containing the links used in the article
+ long_url            = the complete URL of the article
+ news_site           = ???
+ published_time      = time-stamp informing the article was published
+ read_time           = estimated read time for the article
+ sentiments          = calculated sentiment value for a given text
+ short_url           = the shortened URL of the article
+ text                = body of the article 
+ title               = title of the article
