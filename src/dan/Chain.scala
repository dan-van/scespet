package dan;

object Test2 extends App {
  trait Yield[X] {

  }
  trait ActsOn[X] {

  }

  class Chain[P, X](val prev:Chain[_,P], val action:Yield[X]){
    def bind[Y] (f:X => Y):Chain[X,Y] = {
      var funcWrap = new Func[X,Y](f)
      funcWrap.input = action
      return new Chain[X,Y](this, funcWrap)
    }

    override def toString = prev + " -> " + action
  }

  class Func[X,Y](val f:X=>Y) extends ActsOn[X] with Yield[Y] {
    var input:Yield[X] = _
    override def toString = f+"("+input+")"
  }

  implicit def funcToActsOn[X,Y](f:X=>Y) :ActsOn[X] = {
    return new Func(f)
  }

  class Val[X](val x:X) extends Yield[X] {
    override def toString = String.valueOf(x)
  }

  implicit def valToYield[X](x:X) :Yield[X] = {new Val(x)}

  var c = new Chain(null, "source").bind(_+"str1").bind(_+123).bind(_=>"str2")
  println(c)
}