package futuresql.postgres

import futuresql.main.{Message, RowIterator, MessageBuffer}
import play.api.libs.iteratee.Enumerator
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import futuresql.nio.AsyncWriteBuffer
import futuresql.postgres.types.QueryParam
import java.sql.SQLRecoverableException

/**
 * @author Bryce Anderson
 *         Created on 8/1/13
 */
trait PreparedStatementPipeline extends QueryPipeline {

  def query: String
  def params: Seq[QueryParam]
  implicit def ec: ExecutionContext

  def run(): Future[Enumerator[RowIterator]] = {
    val p = Promise[Enumerator[RowIterator]]
    writeQuery(p)
    p.future
  }

  private def writeQuery(p: Promise[Enumerator[RowIterator]]) {
      writebuff.writeBuffers(Array(
        Parse(query).toBuffer,
        Bind()(params).toBuffer,
        Describe("", 'S').toBuffer,
        Execute().toBuffer,
        Sync.toBuffer
      )).onComplete{ _ => finish(p)}
  }

  private def finish(p: Promise[Enumerator[RowIterator]]) {

    var pdesc: ParameterDescription = null

    def getResponses() {
      messagebuff.getMessage().onComplete {
        case Success(ParseComplete) => getResponses()

        case Success(desc: ParameterDescription) =>
          pdesc = desc
          getResponses()

        case Success(BindComplete) => getResponses()

        case Success(desc: RowDescription) =>
          p.success(runRows(desc))

        case Success(EmptyQueryResponse) =>
          p.success(Enumerator())
          onFinished()

        case Success(ErrorResponse(msg, code)) =>
          val failMsg = s"Failed to execute statement. Code $code: $msg"
          failAndCleanup(new SQLRecoverableException(failMsg), p)

        case Success(CommandComplete(msg)) =>
          log("Command complete: " + msg)
          onFinished()
          p.success(Enumerator())

        case Success(m) =>
          val failMsg = s"Failed to execute statement, wrong message: $m"
          failAndCleanup(new Exception(failMsg), p)

        case Failure(t) => failAndCleanup(t, p)
      }
    }

    getResponses()
  }

}
