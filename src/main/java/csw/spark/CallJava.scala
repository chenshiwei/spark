package csw.spark

object CallJava {

  def call(para: String = "spark"): Unit = {
    println(s"hello $para")
  }

  def call = println("hello scala")

  def main(args: Array[String]): Unit = {
    call
    call()
    call("cao")

    val test = new CallScala()
    test.setA("hello java")
    println(test.getA)
  }
}