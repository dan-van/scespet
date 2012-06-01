
/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 10/05/2012
 * Time: 22:52
 * To change this template use File | Settings | File Templates.
 */


class Trade(var price:Double, var qty:Double)
var t:Trade = new Trade(1.2, 100)
//var a = {def x:Double = t.price;def y:Double = t.price * t.qty}
var a = new {def x = t.price;def y:Double = t.price * t.qty}
println(a.x+", "+a.y)
t.price = 2.4
t.qty = 200
println(a.x+", "+a.y)

class In[T](val in:T){
}
var b = new In(t){def x = in.price;def y:Double = in.price * in.qty}

