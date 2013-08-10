package futuresql.postgres

import play.api.libs.iteratee.Enumerator
import futuresql.main.{Message, MessageBuffer, RowIterator}
import scala.concurrent.{ExecutionContext, Promise, Future}
import scala.util.{Failure, Success}

/**
 * @author Bryce Anderson
 *         Created on 8/7/13
 */
trait SimpleQueryPipeline extends QueryPipeline { self =>

  def query: String

  def run(): Future[Enumerator[RowIterator]] = {
    val q = SimpleQuery(query)
    val p = Promise[Enumerator[RowIterator]]
    writebuff.writeBuffer(q.toBuffer).onComplete {
      case Success(_) =>
        messagebuff.getMessage().onComplete {
          case Success(EmptyQueryResponse) =>
            p.success(Enumerator())
            onFinished()

          case Success(CommandComplete(msg)) =>
            log("Command complete: " + msg)
            onFinished()
            p.success(Enumerator())

          case Success(desc: RowDescription) =>  // Getting data. Start to read
            p.success(runRows(desc))

          case Success(other: Message) =>
            log("Found unexpected message: " + other)
          // TODO: this needs reworking to be more consistent.
            p.failure(new Exception("Found unexpected message: " + other))
            self.onUnknownMessage(other)

          case Failure(t) =>
            p.failure(t)
            onFailure("Failed to get query response message", t)

        }

      case Failure(t) => p.complete(Failure(t))
    }

    p.future
  }
}
