package futuresql.postgres.types

import scala.language.implicitConversions

import java.nio.ByteBuffer

import futuresql.main.util.BufferUtils._

/**
 * @author Bryce Anderson
 *         Created on 8/4/13
 */
trait QueryParam {

  def strRepr: String

  def writeBuffer(buff: ByteBuffer) = {
    buff.putInt(strRepr.length)
    buff.put(strRepr.getBytes())
  }

  def wireSize = strRepr.length

  def tooBuffer = {
    val buff = newBuff(strRepr.length + 4)
    writeBuffer(buff)
    buff.flip()
    buff
  }

  override def toString() = s"QueryParam( $strRepr )"
}

object QueryParam {

  implicit def byteArrayToParam(in: Array[Byte]) = new QueryParam {

    def strRepr: String = {
      val buff = new StringBuilder
      in.foreach { b => buff.append("%02X".format(b)) }
      buff.result()
    }

    // 0 = text, 1 = binary
    def formatCode: Short = 0
  }

  implicit def stringToParam(in: String) = new QueryParam {
    def strRepr: String = in
  }

  implicit def intToParam(in: Int) = new QueryParam {
    val strRepr = in.toString
  }
}
