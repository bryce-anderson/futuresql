package futuresql.postgres

import play.api.libs.iteratee.{Enumerator, Input}
import futuresql.main.{BufferingEnumerator, MessageBuffer, Message, RowIterator}
import scala.util.{Failure, Success}
import scala.concurrent.{Promise, Future, ExecutionContext}
import futuresql.nio.AsyncWriteBuffer

/**
 * @author Bryce Anderson
 *         Created on 8/5/13
 */
trait QueryPipeline {

  protected implicit def ec: ExecutionContext

  def messagebuff: MessageBuffer

  def writebuff: AsyncWriteBuffer

  def cancelQuery(): Unit

  def onFinished(): Unit

  protected def onFailure(msg: String, t: Throwable)

  def log(msg: String): Unit

  def run(): Future[Enumerator[RowIterator]]

  def onUnknownMessage(m: Message) {
    log("Found unexpected message: " + m)
    sys.error("Don't know how to respond to message: " + m)
  }

  def runRows(desc: RowDescription): Enumerator[RowIterator] = {
    val (pusher, enum) = BufferingEnumerator.get[RowIterator]
    def cycle() {
      messagebuff.getMessage().onComplete {
        case Success(m: DataRow) =>
          if (!pusher(Input.El(new RowIterator(desc, m)))) cycle()
          else cancelQuery()

        case Success(CommandComplete(msg)) =>
          pusher(Input.EOF)
          onFinished()

        case Success(ErrorResponse(msg, code)) =>
          log(s"Received error code $code: $msg")
          pusher(Input.EOF)
          onFinished()

        case Success(m: Message) => onUnknownMessage(m)

        case Failure(t) =>  onFailure("Failed to parse.", t)
      }
    }

    cycle()
    enum
  }

}
