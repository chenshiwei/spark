package csw.spark.mongo

import com.mongodb.spark.MongoSpark

object MongoTest {
  def main(args: Array[String]): Unit = {
    val sc = Configuration.getSc // Don't copy and paste as its already configured in the shell
    val spark = Configuration.getSparkSession

    // Saving data from an RDD to MongoDB
    import org.bson.Document
    import spark.implicits._
    //    val documents = (1 to 10).map(i => Document.parse(s"{test: $i}")).toList
    //    vspark.createDataFrame(documents,classOf[Document])
    //    MongoSpark.save(ds)

    // Saving data with a custom WriteConfig
    import com.mongodb.spark.config._
    val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))

    val sparkDocuments = sc.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))
    MongoSpark.save(sparkDocuments, writeConfig)

    // Loading and analyzing data from MongoDB
    val rdd = MongoSpark.builder().sparkSession(spark).build().toRDD()

    //    val rdd = MongoSpark.load(sc)
    println(rdd.count)
    println(rdd.first.toJson)

    // Loading data with a custom ReadConfig
    val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val customRdd = MongoSpark.load(sc, readConfig)
    println(customRdd.count)
    println(customRdd.first.toJson)

    // Filtering an rdd in Spark
    val filteredRdd = rdd.filter(doc => doc.getInteger("test") > 5)
    println(filteredRdd.count)
    println(filteredRdd.first.toJson)

    // Filtering an rdd using an aggregation pipeline before passing to Spark
    val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{ $match: { test : { $gt : 5 } } }")))
    println(aggregatedRdd.count)
    println(aggregatedRdd.first.toJson)
  }
}