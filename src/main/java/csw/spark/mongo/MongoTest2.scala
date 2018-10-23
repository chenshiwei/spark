package csw.spark.mongo

import com.mongodb.spark.MongoSpark
import com.mongodb.ServerAddress
import com.mongodb.MongoClient
import com.mongodb.MongoCredential

object MongoTest2 {
  def main(args: Array[String]): Unit = {
    val sc = Configuration.getSc // Don't copy and paste as its already configured in the shell
    val spark = Configuration.getSparkSession

    import org.bson.Document
    import spark.implicits._
    import com.mongodb.spark.config._
    import scala.collection.JavaConversions._
    val client = new MongoClient(new ServerAddress("10.1.62.234"),
      List(MongoCredential.createScramSha1Credential("root", "admin", "Root_123".toCharArray)))

    client.getDatabase("alert").listCollectionNames().foreach(collection => {
      MongoSpark.load(spark, ReadConfig(
        Map(
          "spark.mongodb.input.uri" ->
            "mongodb://root:Root_123@10.1.62.234:27017/admin?authMechanism=SCRAM-SHA-1",
          "database" -> "alert",
          "collection" -> collection
        )
      )).createOrReplaceTempView(s"alert_$collection")
    })
 spark.sql("""SELECT 
   | name,
   | alias,
   | description,
   | discrimination,
   | severity,
   | source,
   | count,
   | status,
   | firstOccurTime,
   | lastOccurTime,
   | closeTime 
   | FROM alert_Incident
   """.stripMargin).show(false)
  }
}