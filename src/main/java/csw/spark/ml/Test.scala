package csw.spark.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans

object Test {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark ML example")
      .getOrCreate()

    // Loads data.
    val dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

    // Trains a k-means model.
    val kmeans = new KMeans().setK(3).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
  }
}