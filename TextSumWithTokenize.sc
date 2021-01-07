val textexample = "Jeden Sonntag beschäftigt sich Heribert Prantl, Kolumnist und Autor der SZ, mit einem Thema, das in der kommenden Woche - und manchmal auch darüber hinaus - relevant ist. Hier können Sie \"Prantls Blick\" auch als wöchentlichen Newsletter bestellen - exklusiv mit seinen persönlichen Leseempfehlungen.\n\nAuf die Frage, wo das Positive bleibt, hat einst Erich Kästner geantwortet: Ja, weiß der Teufel, wo das bleibt... Das war vor neunzig Jahren. Heute steht das Positive fix im Kalender, soeben war wieder der \"Tag des Ehrenamts\". Bundespräsidenten und Bürgermeister preisen an solchen Tagen das \"zivilgesellschaftliche Engagement\" und zitieren den Universalgelehrten Gottfried Wilhelm Leibniz: \"Patrioten sind amtlich Unzuständige, die sich um das Gemeinwohl kümmern.\" Sie zitieren das aus Respekt vor diesen Leuten, aber auch aus nützlichen Erwägungen: Der Staat verlässt sich seit einiger Zeit darauf, dass das, was er als Sozialstaat leisten müsste, von privaten Initiativen geleistet wird.\n\nBesonders krass ist das bei den Tafeln. Es gibt 947 in Deutschland, fast in jeder Stadt. Diese Tafeln sind nicht die Feiertafeln zum siebzigjährigen Jubiläum des Grundgesetzes; es gibt diese Tafeln ja inzwischen auch schon seit über 25 Jahren. Und es sind immer mehr geworden, weil in einem der reichsten Länder der Erde die alltägliche Not wächst. Die Zahl der Tafeln hat sich seit der Einführung der Hartz-IV-Gesetze vervielfacht. An den Tafeln kann man studieren, wie sich die Ungleichheit der Gesellschaft darstellt: Nicht nur Arbeitslose kommen dahin, sondern auch Leute, die vom Lohn ihrer Arbeit nicht leben können. Die Spaltungslinien der Gesellschaft verlaufen nicht mehr nur zwischen arbeitenden und arbeitslosen Menschen. Sie verlaufen kreuz und quer. Auf diesem Kreuz und Quer stehen die Tafeln. In meinem Buch \"Eigentum verpflichtet\" (2019) habe ich darüber geschrieben.\n\n947 Anklagetafeln\n\nJede der 947 Tafeln in Deutschland steht für ein Loch im Sozialstaat. Jede dieser 947 Tafeln zeigt, dass der große Satz \"Eigentum verpflichtet\" nicht den Rang hat, der ihm im Staat des Grundgesetzes eigentlich gebührt. Dieser kleine große Satz ist ein Kernsatz des Grundgesetzes. Er ist die kürzeste Kurzfassung der Einsicht, dass Demokratie nur in und mit einem Sozialstaat zu machen ist - und dass ein Sozialstaat mehr ist als eine Wohlstandszentrifuge, dass er mehr ist als das treuherzige Vertrauen auf die \"Trickle-down-Theorie\", die besagt, dass der Reichtum der Oberschicht angeblich automatisch nach unten sickert. Da tröpfelt nämlich nichts, wenn der Staat keine Kanäle anlegt und für Ausgleich sorgt, indem er die Eigentümer des Wohlstands in die Pflicht nimmt.\n\nEigentum verpflichtet: Man kann nicht sagen, dass die deutsche Politik die zwei Wörter in Artikel 14, mit denen das gemeint ist, in den vergangenen siebzig Jahren als Kernsatz behandelt hätte. Eigentum verpflichtet. Wozu? Reicht es, Lebensmittel, die sonst im Müll landen würden, einer Organisation zu übergeben, die sie dann an Bedürftige verteilt?\n\nEs wäre ein Skandal, wenn es diese Tafeln nicht mehr gäbe. Es ist aber auch ein Skandal, dass es sie geben muss. Was soll man von einem Sozialstaat halten, in dem Menschen ihrer Armut wegen öffentlich Schlange stehen müssen, um billige oder kostenlose Lebensmittel zu bekommen? Was soll man von einem Sozialstaat halten, der sich darauf verlässt, dass es Tafeln gibt, an denen den Bedürftigen eine Art Gnadenbrot serviert wird? Da stehen Obdachlose neben Leuten, die sich gerade noch die Miete leisten können; da stehen Rentnerinnen, die von der Rente nicht leben können, neben Flüchtlingen, die das Asylbewerberleistungsgesetz sehr knapp hält."



def splitSentences(input:String) = {
  //regex splits sentences
  input
    .split("(?<!\\w\\.\\w.)(?<![A-Z][a-z]\\.)(?<=\\.|\\?)\\s")
    .toList
    .zipWithIndex
    .map(x => x.swap)
}

def tokenize(input:(Int,String)):List[(Int,String)]= {

       input._2
         .replace(",","")
         .replace(".","")
         .replace("!","")
         .replace("?","")
         .replace(";","")
         .replace("-","")
         .split("\\s")
         .toList
         .map(x => (input._1,x))

}

def textSum(article:String,keys:List[String]):String = {

  val lowerCaseKeys = keys.map(x => x.toLowerCase)
  val articleSplitBySentence = splitSentences(article)
  val articleSentencesAsMap = articleSplitBySentence.toMap
  val tokenizedSentences = articleSplitBySentence.map(x => tokenize(x)).flatten

    tokenizedSentences
      .map{case (sentenceID,token) => (sentenceID, lowerCaseKeys.contains(token.toLowerCase))}
    .filter{case (sentenceID,isKeyword) => isKeyword} //remove false
    .groupBy{case (sentenceID,containsKeywordBoolean) => sentenceID} //GroupBy SentenceID
    .map{case (sentenceId,listOfContainsKeywordBoolean) => (sentenceId,listOfContainsKeywordBoolean.size) }
    .toSeq //From Map to Sequence
    .sortBy{case (sentenceId,amountOfKeywordsContained) => -amountOfKeywordsContained} //Sort by Size Descending
    .take(3) //Take Top3 SentenceID
    .map{case (sentenceId,amountOfKeywordsContained) => articleSentencesAsMap.getOrElse(sentenceId,"")} //Take Top Three Sentences from "full Map"
    .mkString("")

}

val keywordList = List("tafeln","grundgesetzes","eigentum","sozialstaat","leuten")

textSum(textexample,keywordList)



