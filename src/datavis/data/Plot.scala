package data

import scala.swing.{Component, MainFrame}
import gnu.trove.list.array.{TDoubleArrayList, TLongArrayList}
import org.jfree.data.general.AbstractSeriesDataset
import org.jfree.data.xy.XYDataset
import org.jfree.data.DomainOrder
import scespet.core.{HasVal, VectTerm, MacroTerm}
import org.jfree.chart.renderer.xy.{XYStepRenderer, XYItemRenderer}
import org.jfree.chart.axis.{DateAxis, NumberAxis}
import org.jfree.chart.plot.XYPlot
import java.awt.event.{ActionEvent, ActionListener}
import org.jfree.chart.{ChartPanel, JFreeChart}
import java.awt.Dimension
import gsa.esg.mekon.core.EventGraphObject

import scala.collection.JavaConverters._

/**
 * @version $Id$
 */
object Plot {
  lazy val top = new MainFrame(){
    override def closeOperation() {println("Hiding frame");this.iconify()}
    title="View"
  }

  object TimeSeriesDataset {
  }

  class TimeSeriesDataset() extends AbstractSeriesDataset with XYDataset {
    val datas_x = collection.mutable.MutableList[TLongArrayList]()
    val seriesKeys = collection.mutable.MutableList[Any]()
    val datas_y = collection.mutable.MutableList[TDoubleArrayList]()
    var currentCounts = collection.mutable.MutableList[Int]() // this avoids us exposing new points outside of awt thread
    var cellIndex = 0

    var seriesNameFunc:Any => String = String.valueOf

    def addSeries(key:Any):Int = {
      seriesKeys += key
      val newXCol = new TLongArrayList(2000)
      val newYCol = new TDoubleArrayList(2000)
      datas_x += newXCol
      datas_y += newYCol
      currentCounts += 0
      seriesKeys.size - 1
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

    def getSeriesKey(series: Int) = seriesNameFunc(seriesKeys(series))

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
  def plot[X:Numeric](series:MacroTerm[X]) = {
    val dataset = new TimeSeriesDataset()
    val options = new Options[String,X](dataset)
    options.plot(series)
    plotDataset(dataset)
    options
  }

  def plot[K,X:Numeric](series:VectTerm[K,X]) = {
    val dataset = new TimeSeriesDataset()
    val options = new Options[String,X](dataset)
    for (k <- series.input.getKeys.asScala) {
      options.plot( series(k), k.toString )
    }
    plotDataset(dataset)
    options
  }

  class Options[K,X:Numeric](dataset:TimeSeriesDataset) {
    def seriesNames(keyRender:K=>String = {k:K => k.toString}) {dataset.seriesNameFunc = keyRender.asInstanceOf[Any => String]}

    def plot(stream: MacroTerm[X]) :Options[K,X] = plot(stream, "Series "+(dataset.getSeriesCount+1))

    def plot(stream: MacroTerm[X], name:String) :Options[K,X] = {
      val seriesId = dataset.addSeries(name)
      stream.map(v => {
        val asDouble = implicitly[Numeric[X]].toDouble( v )
        dataset.add(seriesId, stream.env.getEventTime, asDouble)
      })
      this
    }
  }

  private def plotDataset[X: Numeric](dataset: Plot.TimeSeriesDataset) {
    var renderer: XYItemRenderer = new XYStepRenderer()
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
    top.contents = Component.wrap(chartPanel)
    if (top.size == new Dimension(0, 0)) top.pack()
    top.visible = true
  }
}
