package programs

import collection.mutable.ArrayBuffer
import scespet.core._
import scespet.util._


/**
 * test Macro implementation
*/
object Program2 extends App {
  import scala.collection.JavaConverters._

  case class Trade(name:String, price:Double, qty:Int)
  var tradeList = new ArrayBuffer[Trade]()
  tradeList += new Trade("VOD.L", 1.12, 1)
  tradeList += new Trade("VOD.L", 2.12, 10)
  tradeList += new Trade("MSFT.O", 3.12, 2)
  tradeList += new Trade("VOD.L", 4.12, 100)
  tradeList += new Trade("MSFT.O", 5.12, 20)
  tradeList += new Trade("VOD.L", 6.12, 1000)
  tradeList += new Trade("MSFT.O", 7.12, 200)
  tradeList += new Trade("VOD.L", 8.12, 10000)
  tradeList += new Trade("MSFT.O", 9.12, 2000)

  val nameList = new ArrayBuffer[String]()
  nameList += "MSFT.O"
  nameList += "VOD.L"
  nameList += "IBM.N"
  nameList += "IBM.N"
  nameList += "LLOY.L"
  nameList += "IBM.N"
  nameList += "BARC.L"

  val impl: SimpleEvaluator = new SimpleEvaluator()
  var names = IteratorEvents(nameList)
  var trades = IteratorEvents(tradeList)//  def output(prefix:String)(term:VectTerm[_,_]) = term.collapse().map(x => println(prefix + String.valueOf(x)))

  def v1 = {
    val namesExpr: MacroTerm[Sum] = impl.query(trades).map(_.qty).reduce(new Sum).each(3)
    out("sum each 3 elements:"){namesExpr}
  }
  def v2 = { // now with vectors
    var namesExpr = impl.query(trades).by(_.name).map(_.qty).reduceNoMacro(new Sum).each(3)
    out("ewma each 3 elements by name:"){namesExpr}
  }

  def getTrades(name:String) = {
    val tradeStream = impl.query(trades).filter(_.name == name).input
    println(s"Built tradeStream for $name")
    tradeStream
  }

  def v3 = {
//    var start = impl.query(trades)
    val nameStream = impl.query(names)
    val start = nameStream.takef(getTrades).fold_all_noMacro(() => new Collect)
    impl.run()
    println("run finished. Final data = ")
    for (i <- 0 to start.input.getSize() - 1 ) {
      println(s"$i : ${start.input.getKey(i)} = ${start.input.get(i).data}")
    }
  }

  // build a vector of Key -> Set[String] to represent a Feed -> Dictionary.entries
  def v4 = {
    //    var start = impl.query(trades)
    val nameStream = impl.query(names).by( x => x ).map(_.dropRight(2)).map(name => Set(".L", ".O").map(name + _))
    out("name to new tails")(nameStream)
    impl.run()
    println("run finished. Final data = ")
    for (i <- 0 to nameStream.input.getSize() - 1 ) {
      println(s"$i : ${nameStream.input.getKey(i)} = ${nameStream.input.get(i)}")
    }
  }

  // collapse a vector of key -> set[String] into a flattened stream of Set[String] to represent a universe source
  def v5 = {
    //    var start = impl.query(trades)
    var nameSets = impl.query(names).by( x => x ).map(_.dropRight(2)).map(name => Set(".L", ".O").map(name + _))
    val universe = nameSets.collapse().map(_.getValues.asScala.flatten.toSet)
    out("universe stream"){universe}
    impl.run()
    println("run finished. Final data = "+universe.input.value)
  }

  // testing use of "distinct" to flatten a vect[String, Set[String]] into a set of distinct values (like a universe definition)
  def v6 = {
    //    var start = impl.query(trades)
    var nameSets : VectTerm[String, Set[String]] = impl.query(names).by( x => x ).map(_.dropRight(2)).map(name => Set(".L", ".O").map(name + _))
    val universe = nameSets.valueSet[String](_.iterator)
    out("universe stream"){universe}

    impl.run()
    println("run finished. Final data = "+universe.input.getValues)
    val toPrint = universe
    //    for (i <- 0 to nameSets.input.getSize() - 1 ) {
    //      println(s"$i : ${nameSets.input.getKey(i)} = ${nameSets.input.get(i)}")
    //    }
  }

  v6
//  impl.run

//  def foo
//  def fooo()

}
