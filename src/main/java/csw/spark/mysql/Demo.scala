package csw.spark.mysql

/**
  * 作用:
  *
  * @author chensw
  * @since 2019/3/19
  */

import java.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object Demo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark mysql").setMaster("local[4]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()
    val a = System.currentTimeMillis()

    val array = 61.to(90).map(_.toDouble + new Random().nextDouble() * 2).toArray
    println(capacityAnalysis(array,0.1))
    println(capacityAnalysis(array,0.2))
    println(capacityAnalysis(array,0.3))
    println(capacityAnalysis(array,0.4))
    println(capacityAnalysis(array))
    println(capacityAnalysis(array,0.6))

    println(System.currentTimeMillis() - a)

  }

  def capacityAnalysis(array: Array[Double], alpha: Double = 0.5, tendencyUp: Boolean = true,
                               threshold: Double = 100.0): (Double,Double, Double) = {
    val arrayBuffer1: ArrayBuffer[Double] = ArrayBuffer()
    val arrayBuffer2: ArrayBuffer[Double] = ArrayBuffer()
    val n = array.length
    val dca = deepCapacityAnalysis(array, alpha, tendencyUp, threshold)
    val dca1 = deepCapacityAnalysis(array.take(n - 1), alpha, tendencyUp, threshold)
    val dca2 = deepCapacityAnalysis(array.take(n - 2), alpha, tendencyUp, threshold)
    val dca3 = deepCapacityAnalysis(array.take(n - 3), alpha, tendencyUp, threshold)
    val dca4 = deepCapacityAnalysis(array.take(n - 4), alpha, tendencyUp, threshold)
    val dca5 = deepCapacityAnalysis(array.take(n - 5), alpha, tendencyUp, threshold)
    val dca6 = deepCapacityAnalysis(array.take(n - 6), alpha, tendencyUp, threshold)
    arrayBuffer1.append(dca._1)
    arrayBuffer1.append(dca1._1)
    arrayBuffer1.append(dca2._1)
    arrayBuffer1.append(dca3._1)
    arrayBuffer1.append(dca4._1)
    arrayBuffer1.append(dca5._1)
    arrayBuffer1.append(dca6._1)
    arrayBuffer2.append(dca._2)
    arrayBuffer2.append(dca1._2)
    arrayBuffer2.append(dca2._2)
    arrayBuffer2.append(dca3._2)
    arrayBuffer2.append(dca4._2)
    arrayBuffer2.append(dca5._2)
    arrayBuffer2.append(dca6._2)
    (dca._1,dca._2, f"${
      math.max(getR(arrayBuffer1.toArray, Array(1, 2, 3, 4, 5, 6, 7)), math.abs(arrayBuffer2.sum) /
        arrayBuffer2.size.toDouble / arrayBuffer2.map(math.abs).max)
    }%1.3f".toDouble)
  }

  /**
    * 容量分析算法——逆向二次平滑指数法
    *
    * @param array      :已按时间排序，间隔基本上是固定的
    * @param alpha      :二次平滑指数法的平滑系数
    * @param tendencyUp :指标是否向上趋势
    * @param threshold  :容量阈值
    * @return :容量超出阈值的时间(单位:天) 如果-1表示暂时不会有超过阈值的危险
    */
  private def deepCapacityAnalysis(array: Array[Double], alpha: Double, tendencyUp: Boolean, threshold: Double)
  : (Double, Double) = {
    if (array.length < 5 || alpha <= 0 || alpha >= 1) {
      return (-1.0, 0.0)
    }
    var lastIndex: Double = array(0)
    var lastSecIndex: Double = array(0)
    for (data <- array) {
      lastIndex = alpha * data + (1 - alpha) * lastIndex
      lastSecIndex = alpha * lastIndex + (1 - alpha) * lastSecIndex
    }
    val a: Double = 2 * lastIndex - lastSecIndex
    val b: Double = (alpha / (1 - alpha)) * (lastIndex - lastSecIndex)
    if (tendencyUp == (a >= threshold)) return (0.0, 0.0)
    if (tendencyUp == (b < 0) || b == 0) return (-1.0, 0.0)
    (f"${(threshold - a) / b}%1.2f".toDouble, f"$b%1.4f".toDouble)
  }


  def getR(array1: Array[Double], array2: Array[Double]): Double = {
    val mean1 = getMean(array1)
    val mean2 = getMean(array2)
    val new1 = array1.map(_ - mean1)
    val new2 = array2.map(_ - mean2)
    getDot(new1, new2) / (getDist(new1) * getDist(new2))
  }

  def getDot(array1: Array[Double], array2: Array[Double]): Double = {
    array1.zip(array2).map(entry => entry._1 * entry._2).sum
  }

  def getDist(array: Array[Double]): Double = {
    math.sqrt(array.map(a => a * a).sum)
  }

  def getSigma(array: Array[Double]): Double = {
    val mean = getMean(array)
    math.sqrt(getMean(array.map(a => (mean - a) * (mean - a))))
  }

  def getMean(array: Array[Double]): Double = {
//    emptyCheck(array)
    array.sum / array.length.toDouble
  }
}
