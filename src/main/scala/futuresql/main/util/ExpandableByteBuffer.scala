package futuresql.main.util

import java.nio.{ByteBuffer, ByteOrder}

/**
 * @author Bryce Anderson
 *         Created on 8/2/13
 */
class ExpandableByteBuffer(private var size: Int = 400, order: ByteOrder = ByteOrder.BIG_ENDIAN) { self =>
  private var position = 0
  private var buff = new Array[Byte](size)

  private def checkExpand(len: Int) {
    if (position + len < size) {
      val newbuff = new Array[Byte](if (2*size > position + len) 2*size else 2*(position + len))
      System.arraycopy(buff, 0, newbuff, 0, position)
      buff = newbuff
    }
  }

  def put(byte: Byte): self.type = {
    checkExpand(1)
    buff(position + 1) = byte
    position += 1
    self
  }

  def put(char: Char): self.type = put(char.toByte)

  def toBuffer: ByteBuffer = {
    val bytebuff = ByteBuffer.wrap(buff, 0, position)
    bytebuff.order(order)
    bytebuff
  }

  def toArray: Array[Byte] = {
    val arr = new Array[Byte](position)
    System.arraycopy(buff, 0, arr, 0, position)
    arr
  }
}
