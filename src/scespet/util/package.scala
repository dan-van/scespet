package scespet

import scespet.core.{Term, VectTerm, MacroTerm}
import java.util.Date
import gsa.esg.mekon.core.Environment

/**
 * sorry - I'm still working out how to use package objects etc to make sensible namespaces and utilities.
 */
package object util {
  def out(prefix:String):TermPrint = new TermPrint(prefix)

//  implicit class IntToEvents(i:Int) {
//    def events = new Events(i)
//  }

//  val BEFORE = new SliceAlign("BEFORE")
//  val AFTER = new SliceAlign("AFTER")

}

package util {
  /**
   * Sometimes a grouping may define a grouping boundary that occurs atomically with the data being grouped.
   * This flag defines Whether the new group should be created before, or after the new datapoint is coalesced
   */
  sealed class SliceAlign(val name:String)
  object SliceAlign {
    /**
     * Sometimes a grouping may define a grouping boundary that occurs atomically with the data being grouped.
     * This flag defines Whether the new group should be created before, or after the new datapoint is coalesced
     * This means that the Slice occurs before any events are collapsed.
     */
    val BEFORE = new SliceAlign("BEFORE")

    /**
     * Sometimes a grouping may define a grouping boundary that occurs atomically with the data being grouped.
     * This flag defines Whether the new group should be created before, or after the new datapoint is coalesced
     * This means that the Slice occurs before any events are collapsed.
     */
    val AFTER = new SliceAlign("AFTER")
  }

  class TermPrint(val prefix:String) {
    // todo: think about whether Term should get a 'time' field, or TermPrint should have an env?
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
