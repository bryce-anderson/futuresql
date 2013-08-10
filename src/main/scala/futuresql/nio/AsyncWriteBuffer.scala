package futuresql.nio

import java.nio.channels.{CompletionHandler, AsynchronousSocketChannel}

import scala.concurrent.{Await, ExecutionContext, Promise, Future}
import scala.concurrent.duration._
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.lang

/**
 * @author Bryce Anderson
 *         Created on 7/25/13
 */
class AsyncWriteBuffer(channel: AsynchronousSocketChannel, size: Int = 10280)(implicit ec: ExecutionContext) {

  def writeBuffer(buff: ByteBuffer): Future[Int] = {
    val p = Promise[Int]
    channel.write(buff, null: Null, new CompletionHandler[Integer, Null] {
      def completed(result: Integer, attachment: Null) {
        p.success(result.intValue())
      }

      def failed(exc: Throwable, attachment: Null) {
        p.failure(exc)
      }
    })
    p.future
  }

  def writeBuffers(buffs: Array[ByteBuffer]): Future[Long] = {
//    val size = buffs.foldLeft(0)( (i, buff) => i + buff.remaining())
//    val data = new Array[Byte](size)
//
//    // Copy buffers into array
//    buffs.foldLeft(0) { (pos, buff) =>
//      val remaining = buff.remaining()
//      buff.get(data, pos, remaining)
//      pos + remaining
//    }
//
//    writeBuffer(ByteBuffer.wrap(data))
    val p = Promise[Long]
    channel.write(buffs, 0, buffs.length, 0L, TimeUnit.MICROSECONDS, null: Null, new CompletionHandler[java.lang.Long, Null] {
      def completed(result: lang.Long, attachment: Null) {
        p.success(result.longValue())
      }

      def failed(exc: Throwable, attachment: Null) {
        p.failure(exc)
      }
    })
    p.future
  }

  def writeBytes(bytes: Array[Byte]) = writeBuffer(ByteBuffer.wrap(bytes))

  def syncWriteBuffer(buff: ByteBuffer, dur: Duration = Duration.Inf) = Await.result(writeBuffer(buff), dur)

  def syncWriteBytes(bytes: Array[Byte]) = syncWriteBuffer(ByteBuffer.wrap(bytes))
}
