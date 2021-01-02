val full = Map((1 -> "Ihr müsst dringend ins Krankenhaus. ") ,
  (2 -> "Abteilung Neurologie, Psychiatrie. ") ,
  (3 -> "Ich habe Angst um Euer Gehirn. ") ,
  (4 -> "Ihr spieltet gegen die Bayern, als hättet Ihr Euren Verstand verloren. ") ,
  (5 -> "Es war ein furchtbares Spiel. ") ,
  (6 -> "Ihr hattet alle einen Black-out (engl. für Verdunkelung). ") ,
  (7 -> "Ihr habt gespielt wie Tote.") ,
  (8 -> "In der Wissenschaft nennt man das den Ich-Aussetzer. ") ,
  (9 -> "Damit meint man einen Zustand, wo man nicht mehr bei sich ist. ") ,
  (10 -> "Die Ärzte sagen, dass die Durchblutung des Gehirns Schuld hat. ") ,
  (11 -> "Dieses wenige Blut im Gehirn löse Angst aus, Panik.") ,
  (12 -> "Borussia Dortmund hätte gegen die Bayern Volleyball oder Blinde Kuh spielen können, sie hätten immer verloren.Weil sie Angst haben. ") ,
  (13 ->"Angst vor den großen Bayern. ") ,
  (14 ->  "Angst vor der Hexe. Angst vor dem Bösen.") ,
  (15 ->  "Und so machten sich die Spieler von Borussia Dortmund in die Hose."))


val tupleList = List((1, "Krankenhaus"),(2, "Abteilung"),(2, "Neurologie"),(2, "Psychiatrie"),(3, "Angst"),(3, "Gehirn"),(4, "Bayern"),(4, "Verstand"),(   5, "Black-Out"),(5, "Verdunkelung"),(6, "Tote"),(7, "Wissenschaft"),(7, "Ich-Aussetzer"),(8, "Zustand"),(9, "Ärzte"),(9, "Durchblutung"),(   9, "Gehirn"),(9, "Schuld"),(10, "Blut"),(10, "Gehirn"),(10, "Angst"),(10, "Panik"),(11, "Borussia Dortmund"),(11, "Bayern"),(11, "Volleyball"),(  11, "Blinde Kuh"),(12, "Angst"),(13, "Angst"),(13, "Bayern"),(14, "Angst" ),(14, "Hexe"),(15, "Angst"),(15, "Bösen"),(16, "Spieler"),(16, "Borussia Dortmung"),(16, "Hose"))

val keys = List("Bayern","Angst","Borussia Dortmund")


/*
wir haben fünf keywords und einen Text. Bestehend als Format

Keywords als KeywordList = List(String)
Text als SatzMap = Map(SatzID:Int, Satz:String)
Text als TupleList= List(SatzID:Int,Token:String)

Gehe durch TupleListe, für jeden Token schaue ob er ein Keyword ist und Zähle welche Sätze am meisten Keywords enthalten.

 */


tupleList
          .map{case (sentenceID,token) => (sentenceID, keys.contains(token))}
          .filter{case (sentenceID,isKeyword) => isKeyword} //remove false
          .groupBy{case (sentenceID,containsKeywordBoolean) => sentenceID} //GroupBy SentenceID
          .map{case (sentenceId,listOfContainsKeywordBoolean) => (sentenceId,listOfContainsKeywordBoolean.size) }
          .toSeq //From Map to Sequence
          .sortBy{case (sentenceId,amountOfKeywordsContained) => -amountOfKeywordsContained} //Sort by Size Descending
          .take(3) //Take Top3 SentenceID
          .map{case (sentenceId,amountOfKeywordsContained) => full.getOrElse(sentenceId,"")} //Take Top Three Sentences from "full Map"