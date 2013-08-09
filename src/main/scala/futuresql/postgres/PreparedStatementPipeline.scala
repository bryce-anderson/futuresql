package futuresql.postgres

import futuresql.main.{RowIterator, MessageBuffer}
import play.api.libs.iteratee.Enumerator
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import futuresql.nio.AsyncWriteBuffer
import futuresql.postgres.types.QueryParam

/**
 * @author Bryce Anderson
 *         Created on 8/1/13
 */
abstract class PreparedStatementPipeline(query: String, params: QueryParam*)(implicit ec: ExecutionContext) extends QueryPipeline {

  protected def onFailure(msg: String, t: Throwable, p: Promise[Enumerator[RowIterator]])

  def run(): Future[Enumerator[RowIterator]] = {
    val p = Promise[Enumerator[RowIterator]]

    writeQuery(p)
    p.future
  }

  private def writeQuery(p: Promise[Enumerator[RowIterator]]) {
    val parseStatement = Parse(query)   // Don't name these yet
    writebuff.writeBuffer(parseStatement.toBuffer).onComplete {
      case Success(i) =>
        messagebuff.getMessage().onComplete{
          case Success(ParseComplete) => bindStatement(p)

          case Success(ErrorResponse(msg, code)) =>
            val failmsg = s"Prepared statement failed to with code $code: msg"
            onFailure(failmsg, null, p)

          case Failure(t) =>
            onFailure("Prepared statement failed to receive message from messagebuffer.", t, p)

        }
      case Failure(t) =>
        onFailure("Prepared statement failed on output buffer during inQuery parsing.", t, p)
    }
  }

  private def bindStatement(p: Promise[Enumerator[RowIterator]]) {
    val bind = Bind()(params)
    writebuff.writeBuffer(bind.toBuffer).onComplete {
      case Success(i) =>
        messagebuff.getMessage().onComplete {
          case Success(BindComplete) => executeStatement(p)

          case Success(ErrorResponse(msg, code)) =>
            val failmsg = s"Prepared statement failed to with code $code: msg"
            onFailure(failmsg, null, p)

          case Success(m) =>
            val failmsg = s"Unknown message type: $m"
            onFailure(failmsg, null, p)

          case Failure(t) =>
            onFailure("Prepared statement failed to receive message from messagebuffer.", t, p)
        }

      case Failure(t) =>
        onFailure("Prepared statement failed on output buffer during statement binding.", t, p)
    }
  }

  private def executeStatement(p: Promise[Enumerator[RowIterator]]) {
    val exec = Execute()
    writebuff.writeBuffer(exec.toBuffer).onComplete {
      case Success(i) => // Now we need to consume the rows
        ???
    }
  }



}
