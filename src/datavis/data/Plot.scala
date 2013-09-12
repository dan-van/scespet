package data

import scala.swing.{Component, MainFrame}
import gnu.trove.list.array.{TDoubleArrayList, TLongArrayList}
import org.jfree.data.general.AbstractSeriesDataset
import org.jfree.data.xy.XYDataset
import org.jfree.data.DomainOrder
import scespet.core.{VectTerm, MacroTerm}
import org.jfree.chart.renderer.xy.{XYStepRenderer, XYItemRenderer}
import org.jfree.chart.axis.{DateAxis, NumberAxis}
import org.jfree.chart.plot.XYPlot
import java.awt.event.{ActionEvent, ActionListener}
import org.jfree.chart.{ChartPanel, JFreeChart}
import java.awt.Dimension

/**
 * @version $Id$
 */
object Plot {
  lazy val top = new MainFrame(){
    override def closeOperation() {println("Hiding frame");this.iconify()}
    title="View"
  }

  object TimeSeriesDataset {
    def apply[X:Numeric](name:String, stream:MacroTerm[X]) :TimeSeriesDataset = {
      val dataset = new TimeSeriesDataset()
      dataset.addSeries(name)
      stream.map(v => {
        val asDouble = implicitly[Numeric[X]].toDouble( v )
        dataset.add(stream.env.getEventTime, asDouble)
      })
      dataset
    }

    def apply[K,X:Numeric](stream:VectTerm[K,X]) :TimeSeriesDataset = {
      import scala.collection.JavaConversions._
      val dataset = new TimeSeriesDataset()
      for (k <- stream.input.getKeys) {
        dataset.addSeries( k )
      }
      stream.mapVector(v => {
        if (v.getSize > dataset.getSeriesCount) {
          for (i <- dataset.getSeriesCount to v.getSize - 1 ) {
            dataset.addSeries( v.getKey(i) )
          }
        }
        dataset.beginAddRow(stream.env.getEventTime)
        for (y <- v.getValues) {
          dataset.addCell(implicitly[Numeric[X]].toDouble(y))
        }
      })
      dataset
    }
  }

  class TimeSeriesDataset() extends AbstractSeriesDataset with XYDataset {
    val data_x:TLongArrayList = new TLongArrayList()
    val seriesKeys = collection.mutable.MutableList[Any]()
    val datas_y = collection.mutable.MutableList[TDoubleArrayList]()
    var currentCount = 0
    var cellIndex = 0

    var seriesNameFunc:Any => String = String.valueOf

    def addSeries(key:Any) {
      seriesKeys += key
      val newCol = new TDoubleArrayList(data_x.size())
      for (i <- 0 to data_x.size()) newCol.add(Double.NaN)
      datas_y += newCol
    }

    def beginAddRow(x:Long) {
      if (cellIndex != 0) throw new IllegalStateException("Did not previously call addCell for a complete row: "+getSeriesCount)
      data_x.add(x)
    }

    def addCell(y:Double) {
      datas_y(cellIndex).add(y)
      cellIndex = if (cellIndex == datas_y.size - 1) 0 else cellIndex + 1
    }

    def add(x:Long, y:Double) = {
      data_x.add(x)
      if (getSeriesCount != 1) throw new IllegalArgumentException("This dataset has "+getSeriesCount+" series, but only 1 values given")
      datas_y(0).add(y)
    }

    def add(x:Long, ys:Array[Double]) = {
      data_x.add(x)
      if (ys.size != datas_y.size) throw new IllegalArgumentException("This dataset has "+getSeriesCount+" series, but only "+ys.size+" values given")
      for (i <- 0 to ys.size - 1) datas_y(i).add(ys(i))
    }

    def getDomainOrder = DomainOrder.NONE

    def getItemCount(series: Int) = currentCount
    def getX(series: Int, item: Int) = data_x.get(item)

    def getXValue(series: Int, item: Int) = data_x.get(item)

    def getY(series: Int, item: Int) = datas_y(series).get(item)

    def getYValue(series: Int, item: Int) = datas_y(series).get(item)

    def getSeriesCount = datas_y.size

    def getSeriesKey(series: Int) = seriesNameFunc(seriesKeys(series))

    def fireupdate() = {
      if (data_x.size() > currentCount) {
        currentCount = data_x.size()
        fireDatasetChanged()
      }
    }
  }

  // ------------------------------

  def plot[X:Numeric](series:MacroTerm[X])(keyRender:String = "Series" ) {
    val dataset:TimeSeriesDataset = TimeSeriesDataset(keyRender, series)
    plotDataset(dataset)
  }

  def plot[K,X:Numeric](series:VectTerm[K,X]) = {
    val dataset:TimeSeriesDataset = TimeSeriesDataset(series)
    plotDataset(dataset)
    new Options[K,X](dataset)
  }

  class Options[K,X](dataset:TimeSeriesDataset) {
    def seriesNames(keyRender:K=>String = {k:K => k.toString}) {dataset.seriesNameFunc = keyRender.asInstanceOf[Any => String]}
  }
  private def plotDataset[X: Numeric](dataset: Plot.TimeSeriesDataset) {
    var renderer: XYItemRenderer = new XYStepRenderer()
    var range: NumberAxis = new NumberAxis()
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
