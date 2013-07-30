package futuresql.main

import scala.concurrent.{ExecutionContext, Promise, Future}
import scala.collection.mutable
import scala.util.{Success, Failure}
import java.nio.channels.AsynchronousCloseException
import futuresql.nio.AsyncReadBuffer

/**
 * @author Bryce Anderson
 *         Created on 7/28/13
 */
trait MessageBuffer extends MessageFilter {
  def run(): Unit

  def getMessage(): Future[Message]

  def close: Future[Boolean]
}

// A pipeline trait. Methods should stack this and search for messages to filter or handle themselves
trait MessageFilter {
  protected def messageFilter(message: Message): Unit
}

abstract class AsyncMessageBuffer(buff: AsyncReadBuffer, parser: MessageParser)
                                 (implicit ec: ExecutionContext) extends MessageBuffer {

  def onFailure(t: Throwable): Boolean

  private val messageQueue = new mutable.Queue[Message]()
  private val requestQueue = new mutable.Queue[Promise[Message]]()
  private val lock = new AnyRef

  private var _isClosed = false

  def messageFilter(m: Message): Unit = lock.synchronized {
    if(!requestQueue.isEmpty) requestQueue.dequeue().complete(Success(m))
    else messageQueue += m
  }

  def isClosed() = _isClosed

  // This method will starts the AsyncBuffer in that it now constantly queries the buffer for messages
  def run() {
    parser.parseBuffer(buff).onComplete {
      case Success(m) =>  messageFilter(m); run()
      case Failure(t) =>  if(onFailure(t)) run() // let onFailure determine if we should close down
    }
  }

  def close() = lock.synchronized {
    _isClosed = true
    requestQueue.foreach(p => p.tryFailure(new AsynchronousCloseException))
    requestQueue.clear()
    messageQueue.clear()
    buff.close()
  }

  override def getMessage(): Future[Message] = lock.synchronized {
    if (isClosed) {
      Future.failed(new AsynchronousCloseException)
    } else if(!messageQueue.isEmpty) {
      Future.successful(messageQueue.dequeue())
    } else {
      val p = Promise[Message]
      requestQueue += p
      p.future
    }
  }
}
