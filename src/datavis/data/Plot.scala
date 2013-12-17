package data

import scala.swing.{Component, MainFrame}
import gnu.trove.list.array.{TDoubleArrayList, TLongArrayList}
import org.jfree.data.general.AbstractSeriesDataset
import org.jfree.data.xy.XYDataset
import org.jfree.data.DomainOrder
import scespet.core._
import org.jfree.chart.renderer.xy.{XYLineAndShapeRenderer, XYItemRendererState, XYStepRenderer, XYItemRenderer}
import org.jfree.chart.axis.{ValueAxis, DateAxis, NumberAxis}
import org.jfree.chart.plot.{CrosshairState, PlotRenderingInfo, XYPlot}
import java.awt.event.{ActionEvent, ActionListener}
import org.jfree.chart.{ChartPanel, JFreeChart}
import java.awt.{Graphics2D, Dimension}
import gsa.esg.mekon.core.{Environment, EventGraphObject}

import scala.collection.JavaConverters._
import java.awt.geom.Rectangle2D
import org.jfree.chart.entity.EntityCollection

/**
 * @version $Id$
 */
object Plot {
  @volatile var active = false
  private var _chartState : ChartState = _

  lazy val top = new MainFrame(){
    active = true
    override def closeOperation() {
      println("Hiding frame");this.iconify()
      active = false
      Plot.synchronized( Plot.notifyAll() )
    }
    title="View"
  }
  def waitForClose() {
    Plot.synchronized {
      while (active) {
        Plot.wait()
      }
      println("Finished waiting for plot close")
    }
  }

  object TimeSeriesDataset {
  }

  class TimeSeriesDataset() extends AbstractSeriesDataset with XYDataset {
    val datas_x = collection.mutable.MutableList[TLongArrayList]()
    val seriesNames = collection.mutable.MutableList[String]()
    val datas_y = collection.mutable.MutableList[TDoubleArrayList]()
    var currentCounts = collection.mutable.MutableList[Int]() // this avoids us exposing new points outside of awt thread
    var cellIndex = 0


    def addSeries(name:String):Int = {
      seriesNames += name
      val newXCol = new TLongArrayList(2000)
      val newYCol = new TDoubleArrayList(2000)
      datas_x += newXCol
      datas_y += newYCol
      currentCounts += 0
      seriesNames.size - 1
    }

    def add(series:Int, x:Long, y:Double) = {
      datas_x(series).add(x)
      datas_y(series).add(y)
    }

    def getDomainOrder = DomainOrder.ASCENDING

    def getItemCount(series: Int) = currentCounts(series)
    def getX(series: Int, item: Int) = datas_x(series).get(item)

    def getXValue(series: Int, item: Int) = datas_x(series).get(item)

    def getY(series: Int, item: Int) = datas_y(series).get(item)

    def getYValue(series: Int, item: Int) = datas_y(series).get(item)

    def getSeriesCount = datas_x.size

    def getSeriesKey(series: Int) = seriesNames(series)

    def fireupdate() = {
      var fire = false
      for (i <- 0 to getSeriesCount - 1 ) {
        val newCount = Math.min(datas_x(i).size, datas_y(i).size)
        if (currentCounts(i) < newCount) {
          fire = true
          currentCounts(i) = newCount
        }
      }
      if (fire) {
        fireDatasetChanged()
      }
    }
  }

  // ------------------------------
// todo: think about using Evidence to provide X axis (e.g. an Environment for clock)
// todo: rather than relying on MacroTerm being passed here
  def plot[X](series:Term[X], name:String = "Series")(implicit ev:Numeric[X], env:Environment) = {
    val options = new Options[String,X](chartstate.dataset)
    options.plot(series, name)
    options
  }


  def plot[X:Numeric](series:MacroTerm[X]) = {
    val options = new Options[String,X](chartstate.dataset)
    options.plot(series)
    options
  }

  def plot[K,X:Numeric](series:VectTerm[K,X]) = {
    val options = new Options[K,X](chartstate.dataset)
    options.plot(series)
    options
  }

  /**
   * this isn't good design. Think about adding stuff to plots and draw inspiration from the major plotting libraries (and REMEMBER not to go overkill, and just use python for advanced stuff!!!)
   */
  class Options[K,X:Numeric](dataset:TimeSeriesDataset) {
    var keyRender:K=>String = (k) => String.valueOf(k)

    def seriesNames(newKeyRender:K=>String):Options[K,X] = {
      keyRender = newKeyRender
      this
    }

    def plot(stream: VectTerm[K, X]) :Options[K,X] = {
      class DatasetAdder(key:K) extends Reduce[X] {
        val currentCount = dataset.getSeriesCount
        val name = keyRender(key)
        val seriesId = dataset.addSeries(name)
        val env = stream.env

        def add(v: X) = {
          val x = env.getEventTime
          val y = implicitly[Numeric[X]].toDouble( v )
          dataset.add(seriesId, x, y)
        }
      }
      stream.reduce_all(new DatasetAdder(_))
      this
    }

    def plot(stream: MacroTerm[X]) :Options[K,X] = plot(stream, "Series "+(dataset.getSeriesCount+1))(stream.env)

    def plot(stream: Term[X])(implicit env:Environment) :Options[K,X] = plot(stream, "Series "+(dataset.getSeriesCount+1))

    def plot(stream: Term[X], name:String)(implicit env:Environment) :Options[K,X] = {
      val seriesId = dataset.addSeries(name)
      stream.map(v => {
        val asDouble = implicitly[Numeric[X]].toDouble( v )
        dataset.add(seriesId, env.getEventTime, asDouble)
      })
      this
    }
  }

  def chartstate :ChartState = {
    if (_chartState == null) {
      _chartState = new ChartState(new TimeSeriesDataset)
      top.contents = Component.wrap(_chartState.chartPanel)
      if (top.size == new Dimension(0, 0)) top.pack()
      top.visible = true
    }
    _chartState
  }

  class ChartState(val dataset:Plot.TimeSeriesDataset) {
    var renderer = new XYStepRenderer() {
      override def drawItem(g2: Graphics2D, state: XYItemRendererState, dataArea: Rectangle2D, info: PlotRenderingInfo, plot: XYPlot, domainAxis: ValueAxis, rangeAxis: ValueAxis, dataset: XYDataset, series: Int, item: Int, crosshairState: CrosshairState, pass: Int) = {
        super.drawItem(g2, state, dataArea, info, plot, domainAxis, rangeAxis, dataset, series, item, crosshairState, pass)
        if (isItemPass(pass)) {
          var entities: EntityCollection = null
          if (info != null) {
            entities = info.getOwner.getEntityCollection
          }
          drawSecondaryPass(g2, plot, dataset, pass, series, item, domainAxis, dataArea, rangeAxis, crosshairState, entities)
        }
      }
    }
    var range: NumberAxis = new NumberAxis()
    range.setAutoRangeIncludesZero(false)

    var domain: DateAxis = new DateAxis("Time")
    var plot: XYPlot = new XYPlot(dataset, domain, range, renderer)

    new javax.swing.Timer(1000, new ActionListener {
      def actionPerformed(e: ActionEvent) {
        dataset.fireupdate()
      }
    }).start()
    val chart = new JFreeChart("Plot", plot)
    val chartPanel = new ChartPanel(chart)

    def enableShapes() {
      renderer.setShapesVisible(true)
      renderer.setAutoPopulateSeriesFillPaint(true)
      renderer.setBaseShapesFilled(true)
      renderer.setAutoPopulateSeriesOutlineStroke(true)
      renderer.setBaseShapesVisible(true)
    }
  }
}
