val full = "Ihr müsst dringend ins Krankenhaus. Abteilung Neurologie, Psychiatrie. Ich habe Angst um Euer Gehirn. Ihr spieltet gegen die Bayern, als hättet Ihr Euren Verstand verloren. Es war ein furchtbares Spiel. Ihr hattet alle einen Black-out (engl. für Verdunkelung). Ihr habt gespielt wie Tote.In der Wissenschaft nennt man das den Ich-Aussetzer. Damit meint man einen Zustand, wo man nicht mehr bei sich ist. Die Ärzte sagen, dass die Durchblutung des Gehirns Schuld hat. Dieses wenige Blut im Gehirn löse Angst aus, Panik.Borussia Dortmund hätte gegen die Bayern Volleyball oder Blinde Kuh spielen können, sie hätten immer verloren.Weil sie Angst haben. Angst vor den großen Bayern. Angst vor der Hexe. Angst vor dem Bösen.Und so machten sich die Spieler von Borussia Dortmund in die Hose."

val keys = List("Bayern","Angst","Borussia Dortmund")

val token = full.split("\\.")
token.size

val tokenlist = token.map(x => x.split(" "))

tokenlist(14)

val booleanList=
  tokenlist
    .map(y => keys.map(key => y.map(word => {
      word==key
    })))

val arrayOfKeywordCounts = booleanList.map(satz => satz.map(keywordBoolList => keywordBoolList.count(isTrue => isTrue==true)))
// Array aus Liste aus drei Zahlen wobei jede Zahl die Anzahl des jeweiligen keywords ist
// List(0,0,1) = (0xBayern, 0xAngst, 1xBorussaDortmund)

val arrayOfCombinedKeywordCounts = arrayOfKeywordCounts.map(x=>x.sum)
//dieses Array ist jetzt eine Liste aus Zahlen, die die Anzahl der Keywords repräsentiert. der 14. Satz wäre also der wichtigste, weil in diesem Satz gleich zwei keywords drin sind.
// 14. Satz = res1: Array[String] = Array("", Angst, vor, den, großen, Bayern) (weil Angst und Bayern enthalten)
//das ist (Für dieses Ultra Furchtbare sample eigentlich ein gutes Ergebnis, weil es die Limitierungen eines solchen Algortihmuses aufzeigt.
// der gleiche Algo würde bei einem richtigen Journalistischen Text vermutlich gut funktionieren, aber post von wagner ist ja "prosa"



/*
Array[List[Array[Boolean]]]
Jede Liste enthält drei Boolean arrays (eine Array für jedes Keyword)
Gehe durch Array
für jede Liste gehe durch jedes Mitglied der Liste (boolean array) zähle trues)

=> Liste aus Arrays aus 3 Zahlen.
reduce arrays by sum => eine Zahl Pro Liste (Liste == Satz)
Wähle Liste mit höchster Zahl = wichtigster Satz
 */

