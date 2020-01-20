package csw.spark

object WordCount {

  def f1(str: String = ""): Array[String] = {
    return str.split(" ");
  }

  def s1(str: String) = str.split(" ")

  /*
   *  (str:String) => str.split(" ")
   *  (str) => str.split(" ")
   *  str => str.split(" ")
   *  _.split(" ")
   */

  def f2(str: String): (String, Int) = {
    return (str, 1);
  }

  def s2(str: String) = (str, 1)

  /*
   *  (str:String) => (str, 1)
   *  str => (str, 1)
   *  (_, 1)
   */

  def f3(x1: Int, x2: Int): Int = {
    return x1 + x2;
  }

  def s3(x1: Int, x2: Int) = x1 + x2

  /*
   *  (x1: Int, x2: Int) => x1 + x2
   *  (x1, x2) => x1 + x2
   *  _ + _
   */

  def f4(result: (String, Int)): Unit = {
    println(result);
  }

  def s4(result: (String, Int)) = println(result)

  /*
   *  re:(String , Int) => println(re)
   *  re => println(re)
   *  println(_)
   *  println
   */

  def main(args: Array[String]): Unit = {

    import org.apache.spark._

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("WordCount"))

    //    sc.textFile("test").flatMap(JavaUtils.j1).map(f2).reduceByKey(f3).foreach(f4)
    //      sc.textFile("test").flatMap(s1).map(s2).reduceByKey(s3).foreach(s4)

    //     sc.textFile("test").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(println)

    //    sc.textFile("test").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).filter(_._2 > 1).foreach(println)
    //      sc.textFile("test").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //        .map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1)).foreach(println)
    sc.textFile("test").flatMap(_.split(" ")).map(_ -> 1).reduceByKey(_ + _).foreach(println)

  }

}