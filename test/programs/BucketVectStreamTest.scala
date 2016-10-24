package programs

import gsa.esg.mekon.core.EventGraphObject
import org.junit.runner.RunWith
import org.scalatest.junit.{ShouldMatchersForJUnit, AssertionsForJUnit, JUnitRunner}
import org.scalatest.{OneInstancePerTest, BeforeAndAfterEach}
import scespet.core.SliceCellLifecycle.MutableBucketLifecycle
import scespet.core._
import scespet.core.types.{MFunc, IntToEvents}
import scespet.util.SliceAlign._
import scespet.util.{SliceAlign, ScespetTestBase}
import scespet.EnvTermBuilder

/**
 * Created by danvan on 17/04/2014.
 */
@RunWith(classOf[JUnitRunner])
class BucketVectStreamTest extends ScespetTestBase with BeforeAndAfterEach with OneInstancePerTest with AssertionsForJUnit with ShouldMatchersForJUnit {
  
  var env:SimpleEnv = _
  var impl:EnvTermBuilder = _
  var data: Array[Char] = _
  var data_chars: Array[Char] = _
  var data_digit: Array[Char] = _
  var singleStream: MacroTerm[Char] = _
  var stream: VectTerm[String, Char] = _

//  var windowStates :Seq[Boolean] = _
  var inWindow :IndexedSeq[Int] = _
  var windowIndicies :Seq[Seq[Int]] = _
  var windowedData_chars: Seq[Seq[Char]] = _


  override protected def beforeEach() {
    super.beforeEach()
    env = new SimpleEnv
    impl = EnvTermBuilder(env)
    data = "a0b1c2d3e4f5g6h7i8j9k".toCharArray
    data_chars = "abcdefghijk".toCharArray
    data_digit = "0123456789".toCharArray

    windowIndicies = Seq((0 to 5), (10 to 15))
    inWindow = windowIndicies.flatten.toIndexedSeq
    windowedData_chars = windowIndicies.map(idxs => idxs.map(i => data(i)).filter(_.isLetter))

    singleStream = impl.asStream(IteratorEvents(data)((char, i) => i))
    stream = singleStream.by(_.isDigit).mapKeys(b => Some(if (b) "Digit" else "Alpha") )
  }
  override protected def afterEach() {
    env.run()
    super.afterEach()
  }

  def buildVectorWindowStream: VectTerm[String, Boolean] = {
    stream.map(s => {val t = env.getEventTime.toInt; inWindow.contains(t)})
  }

  def buildSingleWindowStream: MacroTerm[Boolean] = {
    singleStream.map(s => {val t = env.getEventTime.toInt; inWindow.contains(t)})
  }


  class Append[X] extends Reducer[X, Seq[X]] {
    println("new appender created")
    var value: Seq[X] = Seq[X]()
    override def add(x: X): Unit = value :+= x
  }

  class OldStyleFuncAppend[X](in:HasVal[X], env:types.Env) extends Bucket with OutTrait[Seq[X]]{
    private var ignoreMore = false
    var value = Seq[X]()
    env.addListener(in.trigger, this)
//    if (in.initialised) {
//      env.fireAfterChangingListeners(this) // do my initialisation
//    }
    override def calculate(): Boolean = {
      if (ignoreMore) {
        println("Ooops, NODEPLOY, what happens here? Why the event?")
      }
      if (env.hasChanged(in.trigger)) {
        append(in.value)
        true
      } else false
    }

    def append(x: X) {
      value :+= x
    }

    override def open(): Unit = value = Nil

    /**
     * called after the last calculate() for this bucket. e.g. a median bucket could summarise and discard data at this point
     * NODEPLOY - rename to Close
     */
    override def close(): Unit = {
      ignoreMore = true
      env.removeListener(in.trigger, this)
      super.close()
    }


  }

  class BindableAppendFunc[X] extends MFunc with AutoCloseable with OutTrait[Seq[X]]{
    var value = Seq[X]()

    def add(x:X) {
      value :+= x
    }
    override def calculate(): Boolean = {
      true
    }

    override def close(): Unit = {
      println("Closed "+getClass+" value = "+value)
    }
  }

  def generateAppendScan(dat:Seq[Char]):Seq[Seq[Char]] = {
    dat.scanLeft(Seq[Char]())(_ :+ _).tail
  }

  test("scan") {
    val out = stream.scan(new Append[Char])
    val expectedDigits = generateAppendScan(data_digit)
    val expectedAlpha = generateAppendScan(data_chars)
    new StreamTest("scan :Digits", expectedDigits, out("Digit"))
    new StreamTest("scan :Alpha", expectedAlpha, out("Alpha"))
  }

  test("reduce") {
    val out = stream.reduce(new Append[Char])
    val expectedDigits = Seq(generateAppendScan(data_digit).last)
    val expectedAlpha = Seq(generateAppendScan(data_chars).last)
    new StreamTest("reduce :Digits", expectedDigits, out("Digit"))
    new StreamTest("reduce :Alpha", expectedAlpha, out("Alpha"))
  }

  test("grouped scan") {
    val out = stream.group(3.events).scan(new Append[Char])
    val expectedDigits = data_digit.grouped(3).map( generateAppendScan(_) ).reduce( _ ++ _ )
    val expectedAlpha = data_chars.grouped(3).map( generateAppendScan(_) ).reduce( _ ++ _ )

    new StreamTest("scan :Digits", expectedDigits, out("Digit"))
    new StreamTest("scan :Alpha", expectedAlpha, out("Alpha"))
  }

  test("grouped reduce") {
    val out = stream.group(3.events).reduce(new Append[Char])
    val expectedDigits = data_digit.grouped(3).map( generateAppendScan(_).last ).toSeq
    val expectedAlpha = data_chars.grouped(3).map( generateAppendScan(_).last ).toSeq

    new StreamTest("reduce :Digits", expectedDigits, out("Digit"))
    new StreamTest("reduce :Alpha", expectedAlpha, out("Alpha"))
  }

  test("vect grouped reduce") {
    // define group boundaries (inclusive)
    val groups = stream.filter(c => Set('b', '5', 'd').contains(c))
    val out = stream.group( groups ).reduce(new Append[Char])
    val expectedDigits = Seq("012345".toList, "6789".toList)
    val expectedAlpha = Seq("ab".toList, "cd".toList, "efghijk".toList)
//
    new StreamTest("reduce :Digits", expectedDigits, out("Digit"))
    new StreamTest("reduce :Alpha", expectedAlpha, out("Alpha"))
  }

  test("vect grouped reduce - exclusive") {
    // define group boundaries (exclusive)
    val groups = stream.filter(c => Set('b', '5', 'd').contains(c))
    val out = stream.group( groups, SliceAlign.BEFORE ).reduce(new Append[Char])
    val expectedDigits = Seq("01234".toList, "56789".toList)
    val expectedAlpha = Seq("a".toList, "bc".toList, "defghijk".toList)
//
    new StreamTest("reduce :Digits", expectedDigits, out("Digit"))
    new StreamTest("reduce :Alpha", expectedAlpha, out("Alpha"))
  }


  test("windowDefinition"){
    val windowStream = buildVectorWindowStream
    val inWindowIndexSet = windowIndicies.flatten.toSet
    val windowState_chars = data.toList.zipWithIndex.filter(e => e._1.isLetter).map(e => inWindowIndexSet.contains(e._2))
    val windowState_digits = data.toList.zipWithIndex.filter(e => e._1.isDigit).map(e => inWindowIndexSet.contains(e._2))
    new StreamTest("windowState: Alpha", windowState_chars, windowStream("Alpha"))
    new StreamTest("windowState: Digit", windowState_digits, windowStream("Digit"))
  }

  test("vect window scan") {
    val out = stream.window( buildVectorWindowStream ).scan(new Append[Char])
    val expectedAlpha = windowedData_chars.map( generateAppendScan(_) ).flatten
    new StreamTest("window scan :Alpha", expectedAlpha, out("Alpha"))
  }

  test("window scan") {
    val out = stream.window( buildSingleWindowStream ).scan(new Append[Char])
    val expectedAlpha = windowedData_chars.map( generateAppendScan(_) ).flatten
    new StreamTest("window scan :Alpha", expectedAlpha, out("Alpha"))
  }

  test("vect windowed reduce") {
    // NODEPLOY - need to further define and test semantics of windows not closing before termination.
    // current test has the second window close before end.
    val out = stream.window( buildVectorWindowStream ).reduce(new Append[Char])
    val expectedAlpha = windowedData_chars.toSeq
    new StreamTest("window scan :Alpha", expectedAlpha, out("Alpha"))
  }

  test("windowed reduce") {
    // NODEPLOY - need to further define and test semantics of windows not closing before termination.
    // current test has the second window close before end.
    val out = stream.window( buildSingleWindowStream ).reduce(new Append[Char])
    val expectedAlpha = windowedData_chars.toSeq
    new StreamTest("window scan :Alpha", expectedAlpha, out("Alpha"))
  }

  // -------- the same tests with a HasVal with binds instead of A Reducer
  test("bind scan") {
    val out = stream.group(env.getTerminationEvent).collapseWith(new BindableAppendFunc[Char])(_.add).all()
//    val out = stream.bindTo(new BindableAppendFunc[Char])(_.add).all()
    val expectedDigits = generateAppendScan(data_digit)
    val expectedAlpha = generateAppendScan(data_chars)
    new StreamTest("scan :Digits", expectedDigits, out("Digit"))
    new StreamTest("scan :Alpha", expectedAlpha, out("Alpha"))
  }

  test("bind reduce") {
    val out = stream.bindTo(key => new BindableAppendFunc[Char])(_.add).last()
    val expectedDigits = Seq(generateAppendScan(data_digit).last)
    val expectedAlpha = Seq(generateAppendScan(data_chars).last)
    new StreamTest("reduce :Digits", expectedDigits, out("Digit"))
    new StreamTest("reduce :Alpha", expectedAlpha, out("Alpha"))
  }

  test("bind grouped scan") {
    val out = stream.group(3.events).collapseWith(new BindableAppendFunc[Char])(_.add).all()
    val expectedDigits = data_digit.grouped(3).map( generateAppendScan(_) ).reduce( _ ++ _ )
    val expectedAlpha = data_chars.grouped(3).map( generateAppendScan(_) ).reduce( _ ++ _ )

    new StreamTest("scan :Digits", expectedDigits, out("Digit"))
    new StreamTest("scan :Alpha", expectedAlpha, out("Alpha"))
  }

  test("bind grouped reduce") {
    val out = stream.group(3.events).collapseWith(new BindableAppendFunc[Char])(_.add).last()
    val expectedDigits = data_digit.grouped(3).map( generateAppendScan(_).last ).toSeq
    val expectedAlpha = data_chars.grouped(3).map( generateAppendScan(_).last ).toSeq

    new StreamTest("reduce :Digits", expectedDigits, out("Digit"))
    new StreamTest("reduce :Alpha", expectedAlpha, out("Alpha"))
  }


// -------------- Pure old-style function streams
  test("MFunc scan") {
    val out = stream.keyToStream( key => impl.buildStream(new OldStyleFuncAppend[Char](stream(key), env)).all() )
    val expectedDigits = generateAppendScan(data_digit)
    val expectedAlpha = generateAppendScan(data_chars)
    new StreamTest("scan :Digits", expectedDigits, out("Digit"))
    new StreamTest("scan :Alpha", expectedAlpha, out("Alpha"))
  }

  test("MFunc reduce") {
    val out = stream.keyToStream( key => impl.buildStream(new OldStyleFuncAppend[Char](stream(key), env)).last() )
    val expectedDigits = Seq(generateAppendScan(data_digit).last)
    val expectedAlpha = Seq(generateAppendScan(data_chars).last)
    new StreamTest("reduce :Digits", expectedDigits, out("Digit"))
    new StreamTest("reduce :Alpha", expectedAlpha, out("Alpha"))
  }

  test("MFunc grouped scan") {
    val out = stream.keyToStream( key => impl.buildStream(new OldStyleFuncAppend[Char](stream(key), env)).reset(3.events).all() )
    val expectedDigits = data_digit.grouped(3).map( generateAppendScan(_) ).reduce( _ ++ _ )
    val expectedAlpha = data_chars.grouped(3).map( generateAppendScan(_) ).reduce( _ ++ _ )

    new StreamTest("scan :Digits", expectedDigits, out("Digit"))
    new StreamTest("scan :Alpha", expectedAlpha, out("Alpha"))
  }

  test("MFunc grouped reduce") {
    // TODO:this is broken
    val out = stream.keyToStream( key => impl.buildStream(new OldStyleFuncAppend[Char](stream(key), env)).reset(3.events).last() )
    val expectedDigits = data_digit.grouped(3).map( generateAppendScan(_).last ).toSeq
    val expectedAlpha = data_chars.grouped(3).map( generateAppendScan(_).last ).toSeq

    new StreamTest("reduce :Digits", expectedDigits, out("Digit"))
    new StreamTest("reduce :Alpha", expectedAlpha, out("Alpha"))
  }
// -------------- tricky composition of self-generator and binding. OldStyleFuncAppend is both doing its own subs to the
  // constructor HasVal, but is also being bound with an 'adder' mutation


  ignore("MFunc bind scan") {
    val alternate:Function1[Char, Boolean] = new Function1[Char,Boolean] {
      var accept = false
      override def apply(v1: Char): Boolean = { accept = !accept; accept }
    }

    val lettersOnly = stream.filter(_.isLetter)
    scespet.util.out("letters") {lettersOnly}
    val alternateLet = lettersOnly.filter(alternate)
    scespet.util.out("alternate") {alternateLet}

    val alternateUppers = alternateLet.map( _.toUpper )
    scespet.util.out("stream") {alternateUppers}

    val output = alternateUppers.bindTo(key => new OldStyleFuncAppend[Char]( stream(key), env))(_.append).all()

    val expectedDigits = generateAppendScan(data_digit)
    val expectedAlpha = List(
      "Aa",
      "Aab",
      "AabCc",
      "AabCcd",
      "AabCcdEe",
      "AabCcdEef",
      "AabCcdEefGg",
      "AabCcdEefGgh",
      "AabCcdEefGghIi",
      "AabCcdEefGghIij",
      "AabCcdEefGghIijKk"
    ).map(_.toCharArray.toSeq)

//    scespet.util.out("Digit") {output("Digit")}
//    scespet.util.out("Alpha") {output("Alpha")  }
    new StreamTest("scan :Digits", expectedDigits, output("Digit"))
    new StreamTest("scan :Alpha", expectedAlpha, output("Alpha"))
  }

  ignore("MFunc bind reduce") {
    val alternate:Function1[Char, Boolean] = new Function1[Char,Boolean] {
      var accept = false
      override def apply(v1: Char): Boolean = { accept = !accept; accept }
    }
    val alternateUppers = stream.filter(_.isLetter).filter( alternate ).map( _.toUpper )

    val out = alternateUppers.bindTo(key => new OldStyleFuncAppend[Char]( stream(key), env))(_.append).last()
    val expectedDigits = Seq(generateAppendScan(data_digit).last)
    val expectedAlpha = List("AabCcdEefGghIijKk".toCharArray.toSeq)

    new StreamTest("scan :Digits", expectedDigits, out("Digit"))
    new StreamTest("scan :Alpha", expectedAlpha, out("Alpha"))
  }

  ignore("MFunc bind grouped scan") {
    val alternate:Function1[Char, Boolean] = new Function1[Char,Boolean] {
      var accept = false
      override def apply(v1: Char): Boolean = { accept = !accept; accept }
    }
    val alternateUppers = stream.filter(_.isLetter).filter( alternate ).map( _.toUpper )

    // build OldStyleFuncAppend, with one stream of events coming from and old-style stream: alternateUppers(key),
    // and other events coming from calls to .append
    val out = stream.group(3.events).collapseWith((key:String) => new OldStyleFuncAppend[Char](alternateUppers(key), env))(_.append).all()

    val expectedDigits = data_digit.grouped(3).map( generateAppendScan(_) ).reduce( _ ++ _ )
    val expectedAlpha = List(
      "aA",
      "aAb",
      "aAbCc",
           "d",
           "dEe",
           "dEef",
               "Gg",
               "Ggh",
               "GghIi",
                    "j",
                    "jKk"
    ).map(_.toCharArray.toSeq)

    new StreamTest("scan :Digits", expectedDigits, out("Digit"))
    new StreamTest("scan :Alpha", expectedAlpha, out("Alpha"))
  }

  ignore("MFunc bind grouped reduce") {
    val alternate:Function1[Char, Boolean] = new Function1[Char,Boolean] {
      var accept = false
      override def apply(v1: Char): Boolean = { accept = !accept; accept }
    }
    val alternateUppers = stream.filter(_.isLetter).filter( alternate ).map( _.toUpper )

    // build OldStyleFuncAppend, with one stream of events coming from and old-style stream: alternateUppers(key),
    // and other events coming from calls to .append
    val out = stream.group(3.events).collapseWithK(key => new OldStyleFuncAppend[Char](alternateUppers(key), env))(_.append).last()

    val expectedDigits = data_digit.grouped(3).map( generateAppendScan(_).last ).toSeq
    val expectedAlpha = List(
      "AabCc",
           "dEef",
               "GghIi",
                    "jKk"
    ).map(_.toCharArray.toSeq)

    new StreamTest("scan :Digits", expectedDigits, out("Digit"))
    new StreamTest("scan :Alpha", expectedAlpha, out("Alpha"))
  }
}
