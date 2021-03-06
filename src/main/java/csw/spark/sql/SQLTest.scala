package csw.spark.sql

import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Properties
import java.io.File
import java.util.concurrent._
import java.util.Collections

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
 * @author chensw
 * @since at 2016-11-30下午3:04:46
 */
object SQLTest {

   def main(args: Array[String]) {

    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val df = spark.read.json("../spark/src/main/resources/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
    df.select(to_json(struct(df.schema.map(h=>col(h.name)): _*)).as("value")).show

    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    df.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // Select only the "name" column
    df.select("name").show()
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     null|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+

    // Select people older than 21
    df.filter($"age" > 21).show()
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+

    // Count people by age
    df.groupBy("age").count().show()
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+


    // Encoders are created for case classes
    import spark.implicits._

    val caseClassDS = Seq(Person("Andy", 32)).toDS()
//    caseClassDS.show()
    // +----+---+
    // |name|age|
    // +----+---+
    // |Andy| 32|
    // +----+---+

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
  }

}
    case class Person(name: String, age: Long)