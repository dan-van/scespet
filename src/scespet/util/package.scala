package scespet

import scespet.core.{Events, Term, VectTerm, MacroTerm}
import java.util.Date
import scespet.expression.CapturedTerm
import gsa.esg.mekon.core.Environment

/**
 * sorry - I'm still working out how to use package objects etc to make sensible namespaces and utilities.
 */
package object util {
  def out(prefix:String):TermPrint = new TermPrint(prefix)

  implicit class IntToEvents(i:Int) {
    def events = new Events(i)
  }

  val BEFORE = new SliceAlign("BEFORE")
  val AFTER = new SliceAlign("AFTER")

}

package util {
  sealed class SliceAlign(val name:String)

  class TermPrint(val prefix:String) {
    // todo: errrm, think about whether Term should get a 'time' field

    def apply[X](term:CapturedTerm[_, X])(implicit env:Environment = null) :CapturedTerm[_, X] = apply[X](term.asInstanceOf[Term[X]])(env).asInstanceOf[CapturedTerm[_, X]]

    //    def apply[X](term:Term[X]) :Term[X] = {
    //      var count = 0
    //      term.map(x => {println( "Event "+count+" "+prefix + String.valueOf(x)); count += 1; x} )
    //    }

    def apply[X](term:Term[X])(implicit env:Environment = null) :Term[X] = {
      var count = 0
      if (env == null) {
        term.map(x => {println( "Event "+count+" "+prefix + String.valueOf(x)); count += 1; x} )
      } else {
        term.map(x => {println( new Date(env.getEventTime) + ": " + String.valueOf(x)); x} )
      }
    }
    def apply[X](term:MacroTerm[X]) :MacroTerm[X] = { term.map(x => {println( new Date(term.env.getEventTime) + ": " + prefix + String.valueOf(x))}); term }
    def apply[K,X](term:VectTerm[K,X]):VectTerm[K,X] = { term.mapVector(x => println(new Date(term.env.getEventTime) + ": " + prefix + String.valueOf(x))); term }
  }

  trait mixin {
    def out(prefix:String):TermPrint = new TermPrint(prefix)
  }
}
