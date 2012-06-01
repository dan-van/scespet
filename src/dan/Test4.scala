import collection.generic.CanBuildFrom

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 10/05/2012
 * Time: 22:52
 * To change this template use File | Settings | File Templates.
 */


class Trade(var price:Double, var qty:Double)
var t:Trade = new Trade(1.2, 100)

//abstract class In[T](){
//  var in:T
//}
object cons {
  def apply[IN, OUT](in:IN, out: IN => OUT):OUT = {
    new Bind(in, out)
    out
  }
}

// class MStream[A] {
//  override final def map[B, That](f: A => B)(implicit bf: CanBuildFrom[Stream[A], B, That]): That = {
// }

  class Bind(val x:Any, val y:Any) {

}
println(cons(t, x => {x.price}))
// conclusion, closer, but coalesce In and Bind, turn into Expr

