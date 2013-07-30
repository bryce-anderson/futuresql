package futuresql.postgres

import java.text.SimpleDateFormat
import java.util.Date
import futuresql.postgres.util.PGOid

/**
 * @author Bryce Anderson
 *         Created on 7/24/13
 */
object TextRowParser {

  def parseString(data: Array[Byte]) = new String(data)

  def parseInt(data: Array[Byte]) = parseString(data).toInt

  def parseLong(data: Array[Byte]) = parseString(data).toLong

  def parseShort(data: Array[Byte]) = parseString(data).toShort

  def parseDouble(data: Array[Byte]) = parseString(data).toDouble

  def parseFloat(data: Array[Byte]) = parseString(data).toFloat

  def parseDate(data: Array[Byte], typeCode: Int): Date = {
    val str = parseString(data)
    val fmtstr = typeCode match {
      case PGOid.T_timestamptz => "yyyy-MM-dd HH:mm:ss-ZZ"
      case PGOid.T_timestamp   => "yyyy-MM-dd HH:mm:ss"
      case PGOid.T_time        => "HH:mm::ss"
      case PGOid.T_timetz      => "HH:mm:ss-ZZ"
      case PGOid.T_date        => "yyyy-MM-dd"
    }
    new SimpleDateFormat(fmtstr).parse(str)
  }

  def parseBool(data: Array[Byte]): Boolean = {
    if (data(0).toChar == 't') true
    else if (data(0).toChar == 'f') false
    else sys.error(s"Cant parse boolean from '" + parseString(data) + "'")
  }
  /* examples taken from golang pc postgres driver.
  //case oid.T_timestamptz:
  //return mustParse("2006-01-02 15:04:05-07", typ, s)
  case oid.T_timestamp:
  return mustParse("2006-01-02 15:04:05", typ, s)
  case oid.T_time:
  return mustParse("15:04:05", typ, s)
  case oid.T_timetz:
  return mustParse("15:04:05-07", typ, s)
  case oid.T_date:
  return mustParse("2006-01-02", typ, s)

} */

}
