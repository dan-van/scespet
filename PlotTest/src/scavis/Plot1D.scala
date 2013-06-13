package scavis

import jhplot._
import java.awt.Color

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 03/05/2013
 * Time: 07:31
 * To change this template use File | Settings | File Templates.
 */
object Plot1D extends App {
  def getData = {
    var data = new P1D()
    var y = 0.0
    for (i <- 0 to 1000) {
      y = y + ( util.Random.nextDouble() - 0.3 )
      data.add(i, y)
    }
    data.setColor(Color.BLUE)
    data
  }

  def plotA {
    var plot = new Plot()
//    plot.visible()
    plot.draw(getData)
    plot.show()
  }

  def plotFreeChart {
    var plot = new HChart()
    plot.add(getData)
    plot.setChartLine()
    plot.visible()
  }

//  plotA
  plotFreeChart
}
