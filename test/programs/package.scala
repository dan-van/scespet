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
    def apply[X](term:MacroTerm[X]) :MacroTerm[X] = { term.map(x => println(prefix + String.valueOf(x))); term }
    def apply[K,X](term:VectTerm[K,X]):VectTerm[K,X] = { term.collapse().map(x => println(prefix + String.valueOf(x))); term }
  }
  def out(prefix:String):TermPrint = new TermPrint(prefix)

}
