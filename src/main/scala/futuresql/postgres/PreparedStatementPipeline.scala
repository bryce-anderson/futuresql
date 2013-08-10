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
abstract class PreparedStatementPipeline(query: String, params: Seq[QueryParam])(implicit ec: ExecutionContext) extends QueryPipeline {

  def run(): Future[Enumerator[RowIterator]] = {
    val p = Promise[Enumerator[RowIterator]]
    writeQuery(p)
    p.future
  }

  private def failAndCleanup(msg: String, t: Throwable, p: Promise[Enumerator[RowIterator]]) {
    p.failure(new Exception(msg, t))
    onFailure(msg, t)
  }

  private def writeQuery(p: Promise[Enumerator[RowIterator]]) {
    val parseStatement = Parse(query)   // Don't name these yet
    writebuff.writeBuffer(parseStatement.toBuffer)
      //.map( i => writebuff.writeBuffer(Describe("", 'S').toBuffer))
      .map{ _ => writebuff.writeBuffer(Sync.toBuffer)}
      .onComplete {
      case Success(i) =>
        messagebuff.getMessage().onComplete{
          case Success(ParseComplete) =>
            messagebuff.getMessage().onComplete{
              case Success(ReadyForQuery(_)) => bindStatement(p)
              case Success(m) =>
                val failmsg = "Failed to get ready for query. Found message " + m
                failAndCleanup(failmsg, new Exception(failmsg), p)

              case Failure(t) =>
                val failmsg = "Failed to get ready for query. Found exception."
                failAndCleanup(failmsg, new Exception(failmsg, t), p)
            }

          case Success(ErrorResponse(msg, code)) =>
            val failmsg = s"Prepared statement failed to with code $code: $msg"
            failAndCleanup(failmsg, new Exception(failmsg), p)

          case Failure(t) =>
            failAndCleanup("Prepared statement failed to receive message from messagebuffer.", t, p)

        }
      case Failure(t) =>
        failAndCleanup("Prepared statement failed on output buffer during inQuery parsing.", t, p)
    }
  }

  private def bindStatement(p: Promise[Enumerator[RowIterator]]) {
    val bind = Bind()(params)
    writebuff.writeBuffer(bind.toBuffer)
      .map{ _ => writebuff.writeBuffer(Sync.toBuffer)}
      .onComplete {
      case Success(i) =>
        messagebuff.getMessage().onComplete {
          case Success(BindComplete) =>
            messagebuff.getMessage().onComplete {
              case Success(ReadyForQuery(_)) => executeStatement(p)
              case Success(m) => ???
              case Failure(t) => ???
            }

          case Success(ErrorResponse(msg, code)) =>
            val failmsg = s"Prepared statement failed to with code $code: $msg"
            failAndCleanup(failmsg, null, p)

          case Success(m) =>
            val failmsg = s"Unknown message type: $m"
            failAndCleanup(failmsg, null, p)

          case Failure(t) =>
            failAndCleanup("Prepared statement failed to receive message from messagebuffer.", t, p)
        }

      case Failure(t) =>
        failAndCleanup("Prepared statement failed on output buffer during statement binding.", t, p)
    }
  }

  private def executeStatement(p: Promise[Enumerator[RowIterator]]) {
    val exec = Execute()
    writebuff.writeBuffer(exec.toBuffer) onComplete  {
      case Success(i) => // Now we need to consume the rows
        messagebuff.getMessage().onComplete {
          case Success(desc: RowDescription) =>
            p.success(runRows(desc))
          // Finish the statementName
            writebuff.writeBuffer(Sync.toBuffer)

          case Success(EmptyQueryResponse) =>
              p.success(Enumerator())
              onFinished()

          case Success(ErrorResponse(msg, code)) =>
            val failMsg = s"Failed to execute statement. Code $code: $msg"
            failAndCleanup(failMsg, new Exception(failMsg), p)

          case Success(CommandComplete(msg)) =>
            log("Command complete: " + msg)
            onFinished()
            p.success(Enumerator())

          case Success(m) =>
            val failMsg = s"Failed to execute statement, wrong message: $m"
            failAndCleanup(failMsg, new Exception(failMsg), p)

          case Failure(t) => failAndCleanup("Prepared statement failed to receive Row Description from message buffer.", t, p)
        }
    }
  }

}
