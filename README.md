# Prerequisite
+ Java 8
+ MongoDB
+ Scala 2.11.12

# Quickstart

+ git clone https://github.com/news-document-pipeline-htw-berlin/Analytics 
+ Add 3 Spark-NLP-Models to main/resources folder (NamedEntityRecognition, StopWordCleaner, Lemmatizer - links are provided below) 
+ cd Analytics
+ Edit inputUri and outputUri in App.class so they refer to where your inputData is stored and where you want to save your processed data 
+ Make sure input MongoDB is structured according to cheat sheet 
+ sbt run

# Links for required NLP Models

+ StopWordCleaner: https://nlp.johnsnowlabs.com/2020/07/14/stopwords_de.html
+ Lemmatizer: https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/models/lemma_de_2.0.8_2.4_1561248996126.zip
+ NamedEntityRecognition: https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/models/entity_recognizer_md_de_2.4.0_2.4_1579722895254.zip


# MongoDB Cheat Sheet

| 0    |    _id  |
| ---- | ---- |
| 1    |  authors    |
| 2    |  crawl_time    |
| 3    |  description    |
| 4    |  departments    |
| 5    |  entities    |
| 6    |  image_links    |
| 7    |  intro    |
| 8    |  keywords    |
| 9    |  keywords_extracted    |
| 10    |  lemmatizer    |
| 11    |  links    |
| 12    |  long_url    |
| 13    |  news_site    |
| 14    |  published_time   |
| 15    |  read_time    |
| 16    |  sentiments    |
| 17    |  short_url    |
| 18    |  text    |
| 19    |  textsum    |
| 20    |  title    |

+ _id                 = unique Hash to identify a single article 
+ authors             = an array containing the authors
+ crawl_time          = time-spamp informing when the article was crawled
+ description         = summery of the text, written by the author
+ departments         = theme based categorisation of the article
+ entities            = named entities and their predicted category (PERson, LOCation, ORGanisation, MISCellaneous)
+ image_links         = an array, containing the links of the images used in the article
+ intro               = introduction text 
+ keywords            = an array, containing keywords given by the author
+ keywords_extracted  = an array, containing keywords extracted by the analytics team
+ lemmatizer          = content of "StopWordCleaner", but tokens are reduced to their root/neutral form
+ links               = an array, containing the links used in the article
+ long_url            = the complete URL of the article
+ news_site           = name of the source
+ published_time      = time-stamp informing the article was published
+ read_time           = estimated read time for the article
+ sentiments          = calculated sentiment value for a given text
+ short_url           = the shortened URL of the article
+ text                = body of the article 
+ textsum             = a generated summarization of the text consisting of three sentences chosen by their calculated significance
+ title               = title of the article
