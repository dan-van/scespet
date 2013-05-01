package scespet

import core.{Term, VectTerm, MacroTerm}

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 26/04/2013
 * Time: 00:53
 * To change this template use File | Settings | File Templates.
 */
package object util {
  class TermPrint(val prefix:String) {
    def apply[X](term:Term[X]) :Term[X] = term.map(x => {println(prefix + String.valueOf(x)); x} )
    def apply[K,X](term:VectTerm[K,X]):VectTerm[K,X] = { term.collapse().map(x => println(prefix + String.valueOf(x))); term }
  }
  def out(prefix:String):TermPrint = new TermPrint(prefix)

}
