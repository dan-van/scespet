
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
abstract class In[T](var in:T){
}

class Bind[T](val src:T) {
  def func2[OUT](f:(T)=>OUT):OUT = {
    var out = f(src)
    return out
  }
}

var out = new Bind(t).func2(new In(_){def x = in.price; def y:Double = in.price * in.qty})
//
println(out)
// conclusion, closer, but coalesce In and Bind, turn into Expr

