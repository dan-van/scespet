
/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 10/05/2012
 * Time: 22:52
 * To change this template use File | Settings | File Templates.
 */


class Trade(var price:Double, var qty:Double)
var t:Trade = new Trade(1.2, 100)

class In[T](val in:T){}
var a = new In(t){def x = in.price; def y:Double = in.price * in.qty}

println(a.x+", "+a.y)
t.price = 2.4
t.qty = 200
println(a.x+", "+a.y)

// conclusion: how to chain a series of "In" expressions (how does the constr arg get set?)

