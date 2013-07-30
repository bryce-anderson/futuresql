package futuresql.main

import java.nio.ByteBuffer

/**
 * @author Bryce Anderson
 *         Created on 7/28/13
 */

trait Message

trait WritableMessage extends Message {
  def toBuffer: ByteBuffer
}