package futuresql.main.util

import java.nio.{ByteOrder, ByteBuffer}

/**
 * @author Bryce Anderson
 *         Created on 7/22/13
 */
object BufferUtils {
  def newBuff(size: Int) = {
    val buff = ByteBuffer.allocate(size)
    buff.order(ByteOrder.BIG_ENDIAN)
    buff
  }

  def putString(buff: ByteBuffer, str: String) {
    buff.put(str.getBytes)
    buff.put(0.toByte)
  }

  def parseString(buff: ByteBuffer):String = {
    val str = new StringBuilder
    var chr = buff.get().toChar
    while(chr != '\0') {
      str.append(chr)
      chr = buff.get().toChar
    }

    str.result()
  }

  def putByte(buff: ByteBuffer, byte: Byte) {
    val b = if(byte < 0) byte + 256 else byte
    putHalfByte(buff, b >> 4)
    putHalfByte(buff, b & 0xf)

  }

  @inline
  private def putHalfByte(buff: ByteBuffer, i: Int) {
    if (i < 10) buff.put((i + '0').asInstanceOf[Byte])
    else buff.put((i + 'a' - 10).asInstanceOf[Byte])
  }

  def setLength(buff: ByteBuffer) {
    val pos = buff.position()
    buff.position(1)
    buff.putInt(pos - 1)
    buff.position(pos)
  }
}
