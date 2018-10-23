package csw.spark

//基本特点和风格：
// 0	可以省略";"，scala一般不用";"结束。
// 1	方法(函数)默认是最后一行为返回，不写"return"。
// 2	没有"static"关键字,只有单例object，其内部全为静态。
// 3	定义变量只有val和var两种，类型只有"[]"而没有"<>"，若变量类型可以推到出来就可以省略。
// 4	函数作为和变量一样的一等公民，可以有内部函数，变量函数，高阶函数(λ表达式)。
// 5	定义函数： def [name]([para1]:[type1]):[typeN] = {}。
//	  e.g.	def call(para: String = "spark"): Unit = {println(s"hello $para")}。
// 6	引入更加简单的元组val tuple=(1,"hao",2.3); 用tuple._i获取元组(从1开始)。
// 7	包定义/文件名/接口类(trait)/函数调用都比java更灵活，而match case比java的switch强大太多。
// 8	隐式转换、型变、模式匹配及其他scala高级用法(今日不谈)。
// 9 风格与java类似，运行于Java平台(jvm)，并兼容现有的Java程序，可与Java互操作。

object ScalaPrimer extends App {

  var unknow: Any = _
  println(unknow) // null
  unknow = "hello scala"
  System.out.println(unknow) // hello scala
  unknow = 1.2
  println(unknow) // 1.2   
  // Double.class (java) => classOf(Double) (scala)

  var a: Long = 4L
  val b = 4
  val c = if (b >= 0) 1 else -1
  val list1 = List[Long](a, b, c)
  def function = "This is var function！"
  // public static String fuction(){return "This is var function！";} 
  println(a + b) // 8
  println(a.==(b)) // true
  println(a max 3L) // 4
  println(c) // 1
  println(list1) // List(4, 4, 1)
  println(function) // This is var function！

  var r1 = 1 to 10
  var r2 = 1 until 10
  val r3 = Range(1, 10, 2)
  println(r1) // Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
  println(r2) // Range(1, 2, 3, 4, 5, 6, 7, 8, 9)
  println(r3) // Range(1, 3, 5, 7, 9)
  for (i <- r3) {
    print(i)
  }
  println // 13579
  for (i <- r2 if (i % 2)==0) print(i)
  println // 2468

  val m = Map("1" -> 1, "2" -> 2)
  println(m("2")) // 2
  println(m.getOrElse("3", 0)) // 0 

  val t = (123, "hao", 2.6, 3L)
  println(t._2 + t._1) // hao123
  println(t._4 - t._3) // 0.39999999999999999
  println(f"${t._4 - t._3}%.2f") //0.40

  def call(skill: String = "java", name: String, age: Int) =
    println(s"$name with skill $skill is $age years old!")
  call("scala", "csw", 31) // csw with skill scala is 31 years old!
  call(age = 26, name = "tiaotiao") // tiaotiao with skill java is 26 years old!

  case class Person(val skill: String, name: String, var age: Int)
  val p = Person("java", "tiaotiao", 26)
  //  p.name = "cxt"
  p.age = 27
  println(s"${p.name} with skill ${p.skill} is ${p.age}")
  val p2 = p.copy(age = 27, name = "cxt")
  println(p2)

  val list = List(1, 2, 3, 4, 5, 6, 7, 8)
  val array = Array(1, 2, 3, 4, 5, 7)
  println(list.sum) // 36
  println(array.length) // 6
  def add(x: Int, y: Int) = x + y
  println(list.reduce(add)) // 36

}