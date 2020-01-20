package csw.spark.mysql

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.control.Breaks._

object TestDbscan {

    def main(args: Array[String]): Unit = {
        val minPts = 10 //密度阈值
        val ePs = 0.05 //领域半径
        //处理输入数据
        val fileName = "D:\\tmp\\dbscan_data2.txt"
        val lines = Source.fromFile(fileName).getLines()
        val points = lines.map(line => {
            //数据预处理
            val parts = line.split(",").map(_.toDouble)
            var vector = Array[Double]()
            for (i <- parts.indices)
                vector ++= Array(parts(i))
            vector
        }).toArray

        println()
        val (cluster, types) = runDBSCAN(points, ePs, minPts)
        println(cluster.mkString("|"))
        println(types.mkString("|"))
        val map: Map[Array[Double], Int] =points.zip(types).toMap
        points.filter(v => map(v) == -1).foreach(x=>println(x.mkString("|")))
        printResult(points, cluster, types)

    }

    def runDBSCAN(data: Array[Array[Double]], ePs: Double, minPts: Int): (Array[Int], Array[Int]) = {
        val types = (for (_ <- data.indices) yield -1).toArray //用于区分核心点1，边界点0，和噪音点-1(即cluster中值为0的点)
        val visited = (for (_ <- data.indices) yield 0).toArray //用于判断该点是否处理过，0表示未处理过
        var number = 1 //用于标记类
        var xTempPoint :Array[Double]= null
        var yTempPoint:Array[Double] = null
        var distance = new Array[(Double, Int)](1)
        var distanceTemp = new Array[(Double, Int)](1)
        val neighPoints = new ArrayBuffer[Array[Double]]()
        var neighPointsTemp = new Array[Array[Double]](1)
        val cluster = new Array[Int](data.length) //用于标记每个数据点所属的类别
        var index = 0
        for (i <- data.indices) {
            //对每一个点进行处理
            if (visited(i) == 0) { //表示该点未被处理
                visited(i) == 1 //标记为处理过
                xTempPoint = data(i) //取到该点
                distance = data.map(x => (vectorDis(x, xTempPoint), data.indexOf(x))) //取得该点到其他所有点的距离Array{(distance,index)}
                neighPoints ++= distance.filter(x => x._1 <= ePs).map(v => data(v._2)) //找到半径ePs内的所有点(密度相连点集合)

                if (neighPoints.length > 1 && neighPoints.length < minPts) {
                    breakable {
                        for (i <- neighPoints.indices) {
                            //此为非核心点，若其领域内有核心点，则该点为边界点
                            val index = data.indexOf(neighPoints(i))
                            if (types(index) == 1) {
                                types(i) = 0 //边界点
                                break
                            }
                        }
                    }
                }
                if (neighPoints.length >= minPts) {
                    //核心点,此时neighPoints表示以该核心点出发的密度相连点的集合
                    types(i) = 1
                    cluster(i) = number
                    while (neighPoints.nonEmpty) { //对该核心点领域内的点迭代寻找核心点，直到所有核心点领域半径内的点组成的集合不再扩大（每次聚类 ）
                        yTempPoint = neighPoints.head //取集合中第一个点
                        index = data.indexOf(yTempPoint)
                        if (visited(index) == 0) {
                            //若该点未被处理，则标记已处理
                            visited(index) = 1
                            if (cluster(index) == 0) cluster(index) = number //划分到与核心点一样的簇中
                            distanceTemp = data.map(x => (vectorDis(x, yTempPoint), data.indexOf(x))) //取得该点到其他所有点的距离Array{(distance,index)}
                            neighPointsTemp = distanceTemp.filter(x => x._1 <= ePs).map(v => data(v._2)) //找到半径ePs内的所有点

                            if (neighPointsTemp.length >= minPts) {
                                types(index) = 1 //该点为核心点
                                for (i <- neighPointsTemp.indices) {
                                    //将其领域内未分类的对象划分到簇中,然后放入neighPoints
                                    if (cluster(data.indexOf(neighPointsTemp(i))) == 0) {
                                        cluster(data.indexOf(neighPointsTemp(i))) = number //只划分簇，没有访问到
                                        neighPoints += neighPointsTemp(i)
                                    }
                                }
                            }
                            if (neighPointsTemp.length > 1 && neighPointsTemp.length < minPts) {
                                breakable {
                                    for (i <- neighPointsTemp.indices) {
                                        //此为非核心点，若其领域内有核心点，则该点为边界点
                                        val index1 = data.indexOf(neighPointsTemp(i))
                                        if (types(index1) == 1) {
                                            types(index) = 0 //边界点
                                            break
                                        }
                                    }
                                }
                            }
                        }
                        neighPoints -= yTempPoint //将该点剔除
                    } //end-while
                    number += 1 //进行新的聚类
                }
            }
        }
        (cluster, types)

    }

    //--------------------------自定义向量间的运算-----------------------------
    //--------------------------向量间的欧式距离-----------------------------
    def vectorDis(v1: Array[Double], v2: Array[Double]): Double = {
        var distance = 0.0
        for (i <- v1.indices) {
            distance += (v1(i) - v2(i)) * (v1(i) - v2(i))
        }
        distance = math.sqrt(distance)
        distance
    }

    def printResult(data: Array[Array[Double]], cluster: Array[Int], types: Array[Int]): Unit = {
//        val result = data.map(v => (cluster(data.indexOf(v)), v)).groupBy(v => v._1) //Map[int,Array[(int,Vector[Double])]]
//        //key代表簇号，value代表属于这一簇的元素数组
//        result.foreach(v => {
//            println("簇" + v._1 + "包含的元素如下:")
//            v._2.foreach(v => println(v._2))
//        })
        //val noise = cluster.zip(data).filter(v => v._1 ==0)
        //noise.foreach(v => types(data.indexOf(v._2)) = -1) //通过簇号0把噪音点在types中赋值-1,数据集中没有包含在任何簇中(即簇号为0)的数据点就构成异常点
//        val pointsTypes = data.map(v => (types(data.indexOf(v)), v)).groupBy(v => v._1) //Map[点类型int,Array[(点类型int,Vector[Double])]]
        //key代表点的类型号，value代表属于这一类型的元素数组
//        pointsTypes.foreach(v => {
//            if (v._1 == 1) println("核心点如下：")
//            else if (v._1 == 0) println("边界点如下：")
//            else println("噪音点如下：")
//            v._2.foreach(v => println(v._2))
//        })
        val resultMat = cluster.zip(types).zip(data) //Array[((Int,Int),Vector[Double])],即Array[((簇Id，类型Id),点向量)]
        resultMat.filter(p=>p._1._2 == -1).foreach(p=>println(p._1,p._2.mkString("[",",","]")))
    }
}
