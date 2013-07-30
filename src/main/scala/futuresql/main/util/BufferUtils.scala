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

  def setLength(buff: ByteBuffer) {
    val pos = buff.position()
    buff.position(1)
    buff.putInt(pos - 1)
    buff.position(pos)
  }
}
