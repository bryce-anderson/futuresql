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

  def parseBlob(coldesc: Column, bytes: Array[Byte]): Array[Byte] = {
    if(coldesc.isbinary == true) return bytes

    if (bytes.length < 2 || bytes(0) != '\\' || bytes(1) != 'x')
      sys.error("Invalid hex format: " + new String(bytes, 0, 2))

    val newbytes = new Array[Byte]((bytes.length-2)/2)
    def parseHex(index: Int): Unit =
      if(index < newbytes.length) {
      val a = gethex(bytes(index*2))
      val b = gethex(bytes(index*2+1))
      newbytes(index) = ((a << 4) | b ).toByte
      parseHex(index+1)
    }
    parseHex(0)
    newbytes
  }

  private def gethex(b: Byte) = {
    // 0-9 == 48-57
    if (b <= 57)  (b - 48).toByte

    // a-f == 97-102
    else if (b >= 97)  (b - 97 + 10).toByte

    // A-F == 65-70
    else (b - 65 + 10).toByte
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
