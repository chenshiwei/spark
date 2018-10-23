package csw.spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.SparkSession


object LRTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark ML example")
      .getOrCreate()

    import spark.implicits._

    val data = spark.sparkContext.textFile("F:\\MavenProject\\mllib\\src\\main\\resources\\mllib\\iris.dat").map(_.split(",")).filter(_ (4) != "class")
      .map(p => Iris(Vectors.dense(p(0).toDouble, p(1).toDouble, p(2).toDouble, p(3).toDouble), p(4).toString)).toDF()
    data.createOrReplaceTempView("iris")
    val df = spark.sql("SELECT * FROM iris WHERE label != 'Iris-setosa'")
    df.map(row => s"${row(1)}:${row(0)}").collect().foreach(println)

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel").fit(df)
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures").fit(df)

    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    //      .collect().foreach(i=>println(i.mkString("|")))

    val lr = new LogisticRegression().setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures").setMaxIter(10)
      .setRegParam(0.3).setElasticNetParam(0.8)

    val labelConverter = new IndexToString()
      .setInputCol("prediction").setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))

    val model = pipeline.fit(training)
    val result = model.transform(test)
    //      .show(200,truncate = false)

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")

    val score = evaluator.evaluate(result)
    result.show(100, truncate = false)
    println(score)

    //    model.clusterCenters.foreach(println)
  }

  case class Iris(features: Vector, label: String)
}