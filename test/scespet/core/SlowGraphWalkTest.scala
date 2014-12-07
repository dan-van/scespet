package scespet.core

import java.util

import gsa.esg.mekon.core.EventGraphObject.Lifecycle
import org.scalatest.junit.{ShouldMatchersForJUnit, AssertionsForJUnit}
import org.scalatest.{BeforeAndAfterEach, Matchers, FunSuite}
import gsa.esg.mekon.core.{Function => MFunc, EventGraphObject}

import collection.JavaConversions._
/**
 * Created by danvan on 22/11/2014.
 */
class SlowGraphWalkTest extends FunSuite with Matchers with AssertionsForJUnit with ShouldMatchersForJUnit with BeforeAndAfterEach {

  class MyNode(name:String)(implicit graph:SlowGraphWalk) extends MFunc {
    var builtSubtree = false
    var changeSet = List[Set[EventGraphObject]]()

    def eventCount = changeSet.size

    override def calculate(): Boolean = {
      val triggers = graph.getTriggers(this)
      changeSet :+= triggers.toSet
      println(this+" event "+eventCount+" . Sources: "+triggers.mkString(", "))
      if (!builtSubtree) {
        builtSubtree = true
        buildSubtree
      }
      true
    }

    def buildSubtree:Unit = {}

    override def toString: String = s"Node:$name"
  }

  test("initialisation") {
    implicit val graph = new SlowGraphWalk

    val rootFunc = new MFunc {
      override def calculate(): Boolean = true
    }
    val l2 = new MyNode("l2")
    val l2a = new MyNode("l2a")
    val l3 = new MyNode("l3")
    val l3a = new MyNode("l3a")
    val l4 = new MyNode("l4")

    val l1 = new MyNode("l1") {
      override def buildSubtree {
        graph.addTrigger(this, l2)
        graph.addTrigger(this, l3)

        graph.addTrigger(l2, l2a)
        graph.addTrigger(l2a, l4)

        graph.addTrigger(l3, l3a)
        graph.addTrigger(l3a, l4)
      }
    }
    graph.addTrigger(rootFunc, l1)
    graph.applyChanges()
    graph.fire(rootFunc)

    l1.eventCount shouldBe 1

    l2.eventCount shouldBe 1
    l2.changeSet(0) should contain only(l1)
    l3.changeSet(0) should contain only(l1)
    l2a.eventCount shouldBe(1)
    l3a.eventCount shouldBe(1)
    l4.changeSet(0) should contain allOf (l2a, l3a)

    graph.fire(rootFunc)
    l2a.eventCount shouldBe(2)
    l3a.eventCount shouldBe(2)

    //    graph.fire(rootFunc)

  }


}
