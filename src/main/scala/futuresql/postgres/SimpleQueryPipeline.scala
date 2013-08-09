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
        messagebuff.getMessage().map {
          case EmptyQueryResponse =>
            p.success(Enumerator())
            onFinished()

          case CommandComplete(msg) =>
            log("Command complete: " + msg)
            onFinished()
            p.success(Enumerator())

          case desc: RowDescription =>  // Getting data. Start to read
            val enum = runRows(desc)
            p.success(enum)

          case other: Message =>
            log("Found unexpected message: " + other); self.onUnknownMessage(other)
        }

      case Failure(t) => p.complete(Failure(t))
    }

    p.future
  }
}
