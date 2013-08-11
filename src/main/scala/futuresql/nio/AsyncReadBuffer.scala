package futuresql.nio

import java.nio.channels.{CompletionHandler, AsynchronousSocketChannel}
import java.nio.{ByteOrder, ByteBuffer}
import scala.concurrent.{ExecutionContext, Promise, Future}
import scala.util.{Failure, Success}

/**
 * @author Bryce Anderson
 *         Created on 7/23/13
 */
class AsyncReadBuffer(channel: AsynchronousSocketChannel, size: Int = 10280)(implicit ec: ExecutionContext) {
  // Setup the bytebuffer
  private var buff = ByteBuffer.allocate(size)
  buff.order(ByteOrder.BIG_ENDIAN)
  buff.flip()

  def close(): Future[Boolean] =  Future.successful(true)

  private def fillBuffer: Future[Int] = {
    val p = Promise[Int]
    buff.clear()
    channel.read(buff, null: Null, new CompletionHandler[Integer, Null] {
      def completed(result: Integer, attachment: Null) {
        buff.flip()
        p.complete(Success(result.intValue()))
      }
      def failed(exc: Throwable, attachment: Null) { p.complete(Failure(exc))}
    })
    p.future
  }

  def getInt(): Future[Int] = {
    val p = Promise[Int]
    if (buff.remaining() >= 4) {
      p.success(buff.getInt)
    } else { p.completeWith(getBuffer(4).map { buff => buff.getInt}) }

    p.future
  }

  def getByte() : Future[Byte] = {
    if (buff.remaining() >= 1) {
        Future.successful(buff.get)
    } else {
      val p = Promise[Byte]
      fillBuffer.onComplete {
        case Failure(t) => p.failure(t)
        case Success(_) => p.completeWith(getByte())
      }
      p.future
    }
  }

  def getChar(): Future[Char] = getByte().map(_.toChar)

  def getBytes(size: Int): Future[Array[Byte]] = {
    val bytes = new Array[Byte](size)

    val p = Promise[Array[Byte]]

    def fillBytes(read: Int) {
      if(buff.remaining >= size - read) { // Already have enough! Good.
        buff.get(bytes, read, size - read)
        p.success(bytes)
      } else {
        val remaining = buff.remaining()
        buff.get(bytes, read, remaining)  // Write what we have
        fillBuffer.onComplete {
          case Failure(t) => p.complete(Failure(t))
          case Success(_) => fillBytes(read + remaining)
        }
      }
    }

    fillBytes(0)
    p.future
  }

  def getBuffer(size: Int): Future[ByteBuffer] = getBytes(size).map{ b =>
    val buff = ByteBuffer.wrap(b)
    buff.order(ByteOrder.BIG_ENDIAN)
    buff
  }

  def getNextFullBuffer(): Future[ByteBuffer] = getInt flatMap(i => getBuffer(i - 4))

  def dumpBuff() = {
    val b = new StringBuilder
    while(buff.remaining() > 0) b.append(buff.get().toChar)
    b.result
  }

  def remaining() = buff.remaining()
}
