object CaseTest {
  val func1:PartialFunction[String,Int]={
    case "one" => 1
    case "two" => 2
    case _ => 0
  }

  def func2(num:String):Int = num match {
    case "one" => 1
    case "two" => 2
    case _ => 0
  }

  val func3:String => Int ={
    case "one" => 1
    case "two" => 2
    case _ => 0
  }

//  val readAppName:Option[String] => String = {
//    case Some(x) => x
//    case None => brodcast_value.getOrElse(appid , appname)
//  }

  def main(args: Array[String]) {
    println(func1("one"))
    println(func2("one"))
    println(func3("one"))
  }

}
