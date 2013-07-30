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

  def getString(): Future[String] = {
    val p = Promise[String]
    val str = new StringBuilder
    def addChar() {
      if (buff.remaining() > 0) { // Have data. Start to fill it
        val chr = buff.get().toChar
        if (chr == '\0') { // Finished
          p.tryComplete(Success(str.result()))
        } else {
          str.append(chr)
          addChar()
        }
      } else { // Need to fill the buffer
        fillBuffer.onComplete {
          case Failure(t) => p.complete(Failure(t))
          case Success(_) => addChar()
        }
      }
    }

    //sync(addChar(), p.future)
    addChar()
    p.future
  }

  def getInt(): Future[Int] = {
    val p = Promise[Int]
    if (buff.remaining() >= 4) {
      p.complete(Success(buff.getInt))
    } else { p.completeWith(getBuffer(4).map { buff => buff.getInt}) }

    p.future
  }

  def getByte() : Future[Byte] = {
    val p = Promise[Byte]

    def getByte() {
      if (buff.remaining() >= 1) {
        p.complete(Success(buff.get))
      } else {
        fillBuffer.onComplete {
          case Failure(t) => p.complete(Failure(t))
          case Success(_) => getByte()
        }
      }
    }

    //sync(getByte(), p.future)
    getByte()
    p.future
  }

  def getChar(): Future[Char] = getByte().map(_.toChar)

  def getBytes(size: Int): Future[Array[Byte]] = {
    val bytes = new Array[Byte](size)

    val p = Promise[Array[Byte]]

    def fillBytes(written: Int) {
      if(buff.remaining >= size - written) { // Already have enough! Good.
        buff.get(bytes, written, size - written)
        p.complete(Success(bytes))
      } else {
        val remaining = buff.remaining()
        buff.get(bytes, written, remaining)  // Write what we have
        fillBuffer.onComplete {
          case Failure(t) => p.complete(Failure(t))
          case Success(_) => fillBytes(written + remaining)
        }
      }
    }

    //sync(fillBytes(0), p.future)
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
    println("Remaining buffer: " + buff.remaining())
    while(buff.remaining() > 0) b.append(buff.get().toChar)
    b.result
  }

  def remaining() = buff.remaining()

}
