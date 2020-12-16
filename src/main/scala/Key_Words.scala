import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.DataFrame

class Key_Words {


  def tf_idf(data: DataFrame):Unit= {

    val hashingTF = new HashingTF().setInputCol("lemmatizer").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(data)
    // alternatively, CountVectorizer can also be used to get term frequency vectors
    featurizedData.show()
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("features").show()
  }
}
