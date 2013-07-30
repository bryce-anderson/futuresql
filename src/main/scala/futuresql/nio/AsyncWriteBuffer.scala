package futuresql.nio

import java.nio.channels.{CompletionHandler, AsynchronousSocketChannel}

import scala.concurrent.{Await, ExecutionContext, Promise, Future}
import scala.concurrent.duration._
import java.nio.ByteBuffer
import scala.util.{Success, Failure}

/**
 * @author Bryce Anderson
 *         Created on 7/25/13
 */
class AsyncWriteBuffer(channel: AsynchronousSocketChannel, size: Int = 10280)(implicit ec: ExecutionContext) {

  def writeBuffer(buff: ByteBuffer): Future[Int] = {
    val p = Promise[Int]
    channel.write(buff, null: Null, new CompletionHandler[Integer, Null] {
      def completed(result: Integer, attachment: Null) { p.complete(Success(result.intValue())) }
      def failed(exc: Throwable, attachment: Null) { p.complete(Failure(exc)) }
    })
    p.future
  }

  def writeBytes(bytes: Array[Byte]) = writeBuffer(ByteBuffer.wrap(bytes))

  def syncWriteBuffer(buff: ByteBuffer, dur: Duration = Duration.Inf) = Await.result(writeBuffer(buff), dur)

  def syncWriteBytes(bytes: Array[Byte]) = syncWriteBuffer(ByteBuffer.wrap(bytes))
}
