package futuresql.postgres

import play.api.libs.iteratee.{Enumerator, Input}
import futuresql.main.{BufferingEnumerator, MessageBuffer, Message, RowIterator}
import scala.util.{Failure, Success}
import scala.concurrent.{Promise, Future, ExecutionContext}
import futuresql.nio.AsyncWriteBuffer
import java.sql.SQLRecoverableException

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

  protected def onFailure(t: Throwable)

  def log(msg: String): Unit

  def run(): Future[Enumerator[RowIterator]]

  protected def failAndCleanup(t: Throwable, p: Promise[Enumerator[RowIterator]]) {
    p.failure(t)
    onFailure(t)
  }

  def onUnknownMessage(m: Message) {
    log("Found unexpected message: " + m)
    onFailure(new SQLRecoverableException("Don't know how to respond to Postgresql message: " + m))
  }

  def runRows(desc: RowDescription): Enumerator[RowIterator] = {
    val (pusher, enum) = BufferingEnumerator[RowIterator]
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
          onFailure(new SQLRecoverableException(s"Error during enumeration, code $code: $msg"))

        case Success(m: Message) => onUnknownMessage(m)

        case Failure(t) =>  onFailure(t)
      }
    }

    cycle()
    enum
  }

}
