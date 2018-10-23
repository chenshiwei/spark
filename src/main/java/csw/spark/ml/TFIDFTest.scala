package csw.spark.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TFIDFTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark ML example")
      .getOrCreate()

    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark and I like spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic regression models are neat")
    )).toDF("id", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")

    val wordsData = tokenizer.transform(sentenceDataFrame)

    wordsData.show(false)

    val hashingTF = new HashingTF()
      .setNumFeatures(2000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("rawFeatures")

    val featurizedData = hashingTF.transform(wordsData)

    featurizedData.show(false)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.show(false)

    //    model.clusterCenters.foreach(println)
  }
}