package csw.spark.ml

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

object CountVectorizerTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark ML example")
      .getOrCreate()

    // Input data: Each row is a bag of words from a sentence or document.

    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel.transform(df).show(false)
  }
}