var line1 = "Abmachung|NNXY	0.0040	Abmachungen"
var line2 =   "Abschluß|NNXY	0.0040	Abschlüße,Abschlußs,Abschlußes,Abschlüßen"
var line3 = "Abstimmung|NN	0.0040	Abstimmungen"
var line4 = "Agilität|NN	0.0040 geschlossenen"
var line5 = "Aktivität|NN	0.0040	Aktivitäten"

var listOfLines = List(line1,line2,line3,line4,line5)

val sentimentMap =
  listOfLines
    .map{x => x.replaceAll("\\|[A-Z]*\\s"," ")}
    .map{x => x.replaceAll(","," ")}
    .map{x => x.split("\\s")}
    .map{x => (x(0),x(1),x.tail.tail)}
    .map{x => (x._2,x._3++List(x._1))}
    .map(x => (x._2.map(y => (x._1,y))))
    .flatten
    .map(x => x.swap)
    .map(x => (x._1,x._2.toDouble))
    .toMap


var testString = "Der Teil-Lockdown mit geschlossenen Restaurants, Museen, Theatern und Freizeiteinrichtungen wird bis zum 10. Januar verlängert. Das haben Bundeskanzlerin Angela Merkel und die Ministerpräsidenten der Länder bei ihren Beratungen am Mittwoch beschlossen, wie die CDU-Politikerin im Anschluss mitteilte. Im Grundsatz bleibt der Zustand, wie er jetzt ist, sagte Merkel.Deutschland ist nach den Worten von Merkel in der Corona-Pandemie noch »sehr weit entfernt« von Zielwerten. Man habe eine sehr hohe Zahl von Todesopfern zu beklagen, sagte Merkel am Mittwochabend in Berlin nach den Beratungen mit den Regierungschefs der Bundesländer. Dies zeige, welche Verantwortung Bund und Länder hätten. Erreicht werden solle ein Wert von 50 Neuinfektionen pro 100 000 Einwohner innerhalb von sieben Tagen, bekräftigte Merkel."

val testTextAsList =
testString
  .replaceAll(",", " ")
  .replaceAll("\\.","")
  .split("\\s")

testTextAsList.map(x => sentimentMap.getOrElse(x,0))
