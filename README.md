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
| 11   |  Array[String]    |
| 12   |  String    |
| 13   |  Date    |
| 14   |  Integer    |
| 15   |  String    |
| 16   |  String    |

