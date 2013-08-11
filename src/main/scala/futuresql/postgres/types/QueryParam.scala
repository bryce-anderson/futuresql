package futuresql.postgres.types

import scala.language.implicitConversions

import java.nio.ByteBuffer

import futuresql.main.util.BufferUtils._
import scala.annotation.implicitNotFound

/**
 * @author Bryce Anderson
 *         Created on 8/4/13
 */


trait QueryParam {

  def strRepr: String

  def wireSize = strRepr.length

  def writeBuffer(buff: ByteBuffer) = {
    buff.putInt(strRepr.length)
    buff.put(strRepr.getBytes())
  }

  def tooBuffer = {
    val buff = newBuff(strRepr.length + 4)
    writeBuffer(buff)
    buff.flip()
    buff
  }

  override def toString() = s"QueryParam( $strRepr )"
}

object QueryParam {

  private def toStrParam[A](in :A) = new QueryParam { val strRepr = in.toString }

  implicit def byteArrayToParam(in: Array[Byte]) = new QueryParam {
    val strRepr = {
      val buff = new StringBuilder
      in.foreach { b => buff.append("%02x".format(b)) }
      buff.result()
    }
  }

  implicit def toParam(in: Int)     = toStrParam(in)
  implicit def toParam(in: Long)    = toStrParam(in)
  implicit def toParam(in: Float)   = toStrParam(in)
  implicit def toParam(in: Double)  = toStrParam(in)
  implicit def toParam(in: String)  = toStrParam(in)
}
