/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 10/05/2012
 * Time: 22:52
 * To change this template use File | Settings | File Templates.
 */


class Trade(var price:Double, var qty:Double)
var t:Trade = new Trade(1.2, 100)

class Func[X,Y]()

class Expr[X](val source:Expr) {
  def map[Y](f:X => Y):Expr[Y] = {
    return new Expr[Y]
  }
  def map[Y](f:Func[X,Y]):Expr[Y] = {
    return new Expr[Y]
  }
}

class Sum extends Func[Double, Double]

new Expr[Trade]().map(_.price).map(new Sum).map(_+1)
// this is typesafe and compiles.
// next experiment, thread reference fields