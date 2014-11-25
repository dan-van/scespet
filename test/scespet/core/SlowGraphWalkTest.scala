package scespet.core

import java.util

import gsa.esg.mekon.core.EventGraphObject.Lifecycle
import org.scalatest.junit.{ShouldMatchersForJUnit, AssertionsForJUnit}
import org.scalatest.{BeforeAndAfterEach, Matchers, FunSuite}
import gsa.esg.mekon.core.{Function => MFunc, EventGraphObject}

/**
 * Created by danvan on 22/11/2014.
 */
class SlowGraphWalkTest extends FunSuite with Matchers with AssertionsForJUnit with ShouldMatchersForJUnit with BeforeAndAfterEach {
  class MyNode(name:String) extends MFunc with Lifecycle {
    var initialised = false
    var builtSubtree = false
    var initialisedInputs: util.Collection[EventGraphObject] = _

    override def init(initialisedInputs: util.Collection[EventGraphObject]): Boolean = {
      initialised = true
      this.initialisedInputs = initialisedInputs
      println(s"$this initialised with inputs: ${initialisedInputs}")
      true
    }

    override def calculate(): Boolean = {
      println(this+" firing")
      if (!builtSubtree) {
        builtSubtree = true
        buildSubtree
      }
      true
    }

    def buildSubtree:Unit = {}

    override def destroy(): Unit = {}

    override def toString: String = s"Node:$name"
  }

  class SimpleFunc(name:String) extends MFunc {
    var count = 0
    override def calculate(): Boolean = {
      count += 1
      true
    }
  }

  test("initialisation") {
    val graph = new SlowGraphWalk

    val rootFunc = new MFunc {
      override def calculate(): Boolean = true
    }
    val l2 = new MyNode("l2")
    val l2a = new SimpleFunc("l2a")
    val l3 = new MyNode("l3")
    val l3a = new SimpleFunc("l3a")
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

    l1.initialised shouldBe true

    l2.initialised shouldBe true
    l2.initialisedInputs should contain(l1)
    l3.initialisedInputs should contain(l1)
    l2a.count shouldBe(1)
    l3a.count shouldBe(1)
    l4.initialisedInputs should contain allOf (l2a, l3a)

    graph.fire(rootFunc)
    l2a.count shouldBe(2)
    l3a.count shouldBe(2)

    //    graph.fire(rootFunc)

  }
}
