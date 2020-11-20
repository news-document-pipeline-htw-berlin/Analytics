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
| 1    |  text    |
| 2    |  document    |
| 3    |  sentence   |
| 4    |  token|
| 5    |  embeddings    |
| 6    |  ner    |
| 7    |  entities    |
| 8    |  normalized    |
| 9    |  StopWordsCleaner    |
| 10   |  lemmatizer    |

+ _id = unique Hash to identify a single article \
+ text = body of the article \
+ sentence = text split into an array of sentences \
+ token = sentences split into tokens (may contain duplicated) \
+ embeddings = ???\
+ ner = named entities and their predicted category (PERson, LOCation, ORGanisation, MISCellaneous)  \
+ normalized = tokens without punctuation, pointing to the sentence they occurred in\
+ StopWordCleaner = tokens without punctuation + stopwords were removed - still pointing to the sentence they occurred in\
+ lemmatizer = content of "StopWordCleaner" but tokens are reduced to their root/neutral form

