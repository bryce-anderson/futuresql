package futuresql.main

import futuresql.postgres.{DataRow, RowDescription, TextRowParser}
import java.util.Date
import futuresql.postgres.util.PGOid


/**
 * @author Bryce Anderson
 *         Created on 7/24/13
 */
class RowIterator(disc: RowDescription, row: DataRow) extends Iterator[String] {

  private val data = disc.columns.iterator.zip(row.columns.iterator)

  def dataMap: Map[String, String] = data.map{ case (col, bytes) => (col.name,TextRowParser.parseString(bytes)) }.toMap

  def hasNext: Boolean = data.hasNext

  def next(): String = TextRowParser.parseString(nextBytes())

  private def nextBytes(): Array[Byte] = {
    data.next()._2
  }

  def nextString() = next()

  def nextInt(): Int = TextRowParser.parseInt(nextBytes())

  def nextLong(): Long = TextRowParser.parseLong(nextBytes())

  def nextShort(): Short = TextRowParser.parseShort(nextBytes())

  def nextFloat(): Float = TextRowParser.parseFloat(nextBytes())

  def nextDouble(): Double = TextRowParser.parseDouble(nextBytes())

  def nextDate(): Date = {
    val (col, bytes) = data.next()

    TextRowParser.parseDate(bytes, col.dtype)
  }

  def nextBlob(): Array[Byte] = {
    val (col, bytes) = data.next()                         // TODO: is there a code for this data type?
    TextRowParser.parseBlob(col, bytes)
  }

}
