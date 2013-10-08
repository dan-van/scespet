package scespet

import core.{Term, VectTerm, MacroTerm}
import java.util.Date

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 26/04/2013
 * Time: 00:53
 * To change this template use File | Settings | File Templates.
 */
package object util {
  class TermPrint(val prefix:String) {
    // todo: errrm, think about whether Term should get a 'time' field
    def apply[X](term:Term[X]) :Term[X] = {
      var count = 0
      term.map(x => {println( "Event "+count+" "+prefix + String.valueOf(x)); count += 1; x} )
    }
    def apply[X](term:MacroTerm[X]) :Term[X] = term.map(x => {println( new Date(term.env.getEventTime) + ": " + prefix + String.valueOf(x)); x} )
    def apply[K,X](term:VectTerm[K,X]):VectTerm[K,X] = { term.mapVector(x => println(prefix + String.valueOf(x))); term }
  }
  def out(prefix:String):TermPrint = new TermPrint(prefix)

}
