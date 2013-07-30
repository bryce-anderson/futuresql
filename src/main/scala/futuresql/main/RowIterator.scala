package futuresql.main

import futuresql.postgres.{DataRow, RowDescription, TextRowParser}
import java.util.Date
import futuresql.postgres.util.PGOid


/**
 * @author Bryce Anderson
 *         Created on 7/24/13
 */
class RowIterator(disc: RowDescription, row: DataRow) extends Iterator[String] {

  private val data = disc.columns.iterator.zipWithIndex

  def dataMap: Map[String, String] = data.map{ case (col, i) => (col.name,TextRowParser.parseString(row.columns(i))) }.toMap

  def hasNext: Boolean = data.hasNext

  def next(): String = TextRowParser.parseString(nextBytes())

  private def nextBytes(): Array[Byte] = {
    val (_, i) = data.next()
    row.columns(i)
  }

  def nextString() = next()

  def nextInt(): Int = TextRowParser.parseInt(nextBytes())

  def nextLong(): Long = TextRowParser.parseLong(nextBytes())

  def nextShort(): Short = TextRowParser.parseShort(nextBytes())

  def nextFloat(): Float = TextRowParser.parseFloat(nextBytes())

  def nextDouble(): Double = TextRowParser.parseDouble(nextBytes())

  def nextDate(): Date = {
    val (col, i) = data.next()

    TextRowParser.parseDate(row.columns(i), col.dtype)
  }

  def nextBlob(): Array[Byte] = nextBytes() // TODO: is there a code for this data type?

}
