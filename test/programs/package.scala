import scespet.core.{VectTerm, MacroTerm}

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 04/04/2013
 * Time: 20:54
 * To change this template use File | Settings | File Templates.
 */
package object programs {
  class TermPrint(val prefix:String) {
    def apply(term:MacroTerm[_]):Unit = term.map(x => println(prefix + String.valueOf(x)))
    def apply(term:VectTerm[_,_]):Unit = term.collapse().map(x => println(prefix + String.valueOf(x)))
  }
  def out(prefix:String):TermPrint = new TermPrint(prefix)

}
