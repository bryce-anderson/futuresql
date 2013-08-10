package futuresql.postgres.types

import scala.language.implicitConversions

import java.nio.ByteBuffer

import futuresql.main.util.BufferUtils._

/**
 * @author Bryce Anderson
 *         Created on 8/4/13
 */
trait QueryParam {
  def writeBuffer(buff: ByteBuffer)
  def wireSize: Int
  def formatCode: Short // 0 = text, 1 = binary
  def strRepr: String

  def tooBuffer = {
    val buff = newBuff(wireSize)
    writeBuffer(buff)
    buff.flip()
    buff
  }

  override def toString() = s"QueryParam( $strRepr )"
}

object QueryParam {

  implicit def byteArrayToParam(in: Array[Byte]) = new QueryParam {
    def writeBuffer(buff: ByteBuffer) {
      buff.put('\\'.toByte)
      buff.put('x'.toByte)
      for (b <- in) putByte(buff, b)
    }

    def wireSize: Int = in.length + 2

    def strRepr: String = {
      val buff = new StringBuilder
      in.foreach { b => buff.append("%02X".format(b)) }
      buff.result()
    }

    // 0 = text, 1 = binary
    def formatCode: Short = 0
  }

  def stringToParam(in: String) = new QueryParam {
    def formatCode: Short = 0

    def wireSize: Int = in.length + 1

    def writeBuffer(buff: ByteBuffer) {
      putString(buff, in)
    }

    // 0 = text, 1 = binary
    def strRepr: String = in
  }

//  implicit def anyToParam[A](in: A) = new QueryParam {
//    private lazy val bytes = in.toString.getBytes
//    val wireSize = bytes.length
//    def formatCode = 0
//    def writeBuffer(buff: ByteBuffer) {
//      buff.put(bytes)
//    }
//
//    // 0 = text, 1 = binary
//    def strRepr: String = in.toString
//  }
}
