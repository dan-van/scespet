package data

import swing.{ScrollPane, MainFrame, Frame, model}
import javax.swing.table.AbstractTableModel
import java.awt.Dimension
import swing.ScrollPane.BarPolicy
import swing.Table.AutoResizeMode

/**
 * @author: danvan
 * @version $Id$
 */

object TableView {
  val table = new Table()
  table.peer.setAutoCreateRowSorter(true)
  table.autoResizeMode = AutoResizeMode.Off
  val scrollPane = new ScrollPane(table)
  scrollPane.horizontalScrollBarPolicy = BarPolicy.Always

  val top = new MainFrame(){
    override def closeOperation() {println("Hiding frame");this.iconify()}
    title="TableView"
    contents=scrollPane
  }

  def showTable(model:AbstractTableModel) {
    table.model = model
    if (top.size == new Dimension(0,0)) top.pack()
    top.visible = true
  }

  def showCol(name:String, column:Seq[Any]){
    showCols(Array(name):_*)(Array(column):_*)
  }

  def showCols(columnNames:String*)(columns:Seq[Any]*){
    showTable( new ColumnData(columnNames, columns) )
  }
}

class Table() extends scala.swing.Table {
  override def apply(row: Int, column: Int): Any = model.getValueAt(viewToModelRow(row), viewToModelColumn(column))
  def viewToModelRow(idx: Int) = peer.convertRowIndexToModel(idx)
  def modelToViewRow(idx: Int) = peer.convertRowIndexToView(idx)
}

class ColumnData(columnNames:Seq[String], columnData:Seq[Seq[Any]]) extends AbstractTableModel {
  val rowCount = columnData(0).length
  override def getColumnName(column: Int) = columnNames(column).toString
  def getRowCount() = rowCount
  def getColumnCount() = columnData.length
  def getValueAt(row: Int, col: Int): AnyRef = columnData(col)(row).asInstanceOf[AnyRef]
}
