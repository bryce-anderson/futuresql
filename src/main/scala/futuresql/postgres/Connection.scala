package futuresql.postgres


import scala.concurrent.{Await, Promise, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer

import java.nio.channels.{CompletionHandler, AsynchronousSocketChannel}
import java.net.InetSocketAddress
import java.util.concurrent.ExecutionException
import java.io.{StringWriter, IOException}

import play.api.libs.iteratee.{Enumerator}

import futuresql.nio.{AsyncWriteBuffer, AsyncReadBuffer}
import futuresql.main._
import scala.util.Failure
import scala.util.Success
import futuresql.postgres.types.QueryParam
import java.sql.SQLRecoverableException

/**
 * @author Bryce Anderson
 *         Created on 7/25/13
 */


object Connection {
  def newConnection(user: String, passwd: String, address: String, port: Int, db: String)
                   (recycle: Connection => Any = c => println(s"Recycling connection $c") )
                   (onError: (Connection, Throwable) => Any = (_, _) => Unit)(implicit inec: ExecutionContext) = {

    def simpleFactory(): AsynchronousSocketChannel = {
      try {
        val channel = AsynchronousSocketChannel.open()
        channel.connect(new InetSocketAddress(address, port)).get()
        channel
      } catch {
        case exec: ExecutionException => throw exec
        case io: IOException => throw io
      }
  }

    new Connection { self =>

      def ec = inec

      def login = Login(user, passwd, db)

      def newChannel() = simpleFactory()

      def onDeath(conn: Connection, t: Throwable) = onError(conn,t)

      def recycleConnection(conn: Connection) = recycle(conn)

      def log(msg: String) = println(s"Connection $self: DEBUG: $msg")
    }
  }
}


private[postgres] trait Connection { self =>

  def login: Login

  implicit def ec: ExecutionContext

  def newChannel(): AsynchronousSocketChannel

  def onDeath(conn: Connection, t: Throwable): Unit

  def recycleConnection(conn: Connection): Any

  def log(msg: String)

  private var _isClosed = false

  private val defaultRecycleTries = 10

  private val (channel, messagebuff, writebuff) = {
    val channel = newChannel()
    val writebuff = new AsyncWriteBuffer(channel)
    val readbuff = new AsyncReadBuffer(channel)
    val parser = new PostgresMessageParser()
    val messagebuff = new AsyncMessageBuffer(readbuff, parser) {

      override def messageFilter(message: Message) = message match {
        case NoticeResponse(tpe, msg) =>  log(s"Notice type '$tpe': $msg")
        case ps: ParameterStatus => onParamStatus(ps)
        case message => super.messageFilter(message)
      }

      def onFailure(t: Throwable): Boolean = {
        onDeath(self, t)
        false
      }
    }

    messagebuff.run()

    (channel, messagebuff, writebuff)
  }

  val (keyData, options) = startupSeq()

  def onParamStatus(ps: ParameterStatus) { /* TODO: Something should be done here. */ }

  private def startupSeq(): (BackendKeyData, List[ParameterStatus]) = {
    val buff = login.initiationBuffer
    val size = buff.position()
    val count = writebuff.syncWriteBuffer(buff, 5.seconds)
    if (count < size)  sys.error("Failed to connect to database!")
    val resp = Await.result(messagebuff.getMessage(), 5.seconds)
    log(s"Server sent response: $resp")

    val next = resp match {
      case AuthMD5(salt) => login.authBuffer(salt)
      case e => sys.error(s"Found invalid login mechanism: " + e)
    }

    writebuff.syncWriteBuffer(next, 5.seconds)

    val auth = Await.result(messagebuff.getMessage(), 5.seconds)
    log("Received authorization: " + auth)

    if (!auth.isInstanceOf[AuthOK.type]) sys.error("Authorization failed. Received " + auth)

    def getParams(): Future[(BackendKeyData, List[ParameterStatus])] = {
      val paramsBuff = new ListBuffer[ParameterStatus]
      val p = Promise[(BackendKeyData, List[ParameterStatus])]
      var keydata: BackendKeyData = null

      def getParams() {
        messagebuff.getMessage() onComplete {
          case Success(p: ParameterStatus) => paramsBuff += p; getParams()

          case Success(b: BackendKeyData) => keydata = b; getParams()

          case Success(r: ReadyForQuery) => p.complete(Success((keydata, paramsBuff.result())))

          case Failure(t) => sys.error(s"Future returned a failure: $t\n" +
            t.getStackTrace.foldLeft(new StringBuilder){ (b, t) =>
              b.append("\tat ")
              b.append(t.toString)
              b.append('\n')
              b
            }.result())

          case Success(e) => sys.error(s"Found wrong message: $e")
        }
      }

      getParams()
      p.future
    }

    val status = Await.result(getParams(), 5.seconds)
    status
  }

  private def cleanAndRecycle(tries: Int): Unit = {
    if(tries > 0) {
      messagebuff.getMessage().onComplete {
        case Success(ReadyForQuery(_)) =>
          recycleConnection(self)

        case Success(m: Message) =>
          log("Cleaned message:" + m)
          cleanAndRecycle(tries - 1)

        case Failure(t) => onDeath(self, t)
      }
    } else onDeath(self, new Exception("Cleans exceeded during cleanAndRecycle."))
  }

  private def cancelQuery() {
    log("Canceling Query.")
    try {
      val tempChannel = newChannel()
      tempChannel.write(CancelRequest(keyData).toBuffer, null: Null, new CompletionHandler[Integer, Null] {
        def completed(result: Integer, attachment: Null) {
          tempChannel.close()
        }
        def failed(exc: Throwable, attachment: Null) {
          tempChannel.close()
        }
      })
    } catch {
      case t: Throwable => // Don't care
    } finally  cleanAndRecycle(500)  // TODO: How many messages to clean on a canceled query?
  }

  private abstract class PipelineUtils(val query: String) {
    def writebuff                   = self.writebuff
    def messagebuff: MessageBuffer  = self.messagebuff
    def log(msg: String)            = self.log(msg)
    def ec: ExecutionContext        = self.ec
    def cancelQuery()               = self.cancelQuery()
    def onFinished()                = self.cleanAndRecycle(defaultRecycleTries)
    def onFailure(t: Throwable)     = t match {
      case t: SQLRecoverableException =>
        log("Caught recoverable exception: " + t)
        cleanAndRecycle(defaultRecycleTries)

      case t => self.onDeath(self, t)
    }
  }

  def query(query: String): Future[Enumerator[RowIterator]] = {
    val pipeline = new PipelineUtils(query) with SimpleQueryPipeline
    pipeline.run()
  }

  def preparedQuery(query: String, inparams: Seq[QueryParam]): Future[Enumerator[RowIterator]] = {
    val pipeline = new PipelineUtils(query) with PreparedStatementPipeline { def params: Seq[QueryParam] = inparams }
    pipeline.run()
  }

  def isClosed() = _isClosed

  def close() {
    _isClosed = true
    writebuff.writeBuffer(Terminate.toBuffer)
             .flatMap( _ => messagebuff.close())
             .onComplete{ _ =>
                log("Closing Channel")
                channel.close()
              }
  }

  def handleUnexpected(msg: Message, p: Promise[_], stage: String) = msg match {
    case e: ErrorResponse =>
      val ex = new Exception(s"Failed query in stage $stage with error code '${e.errorcode}'. Msg: ${e.msg}")
      p.complete(Failure(ex))

    case other => p.complete(Failure(new Exception(s"Don't know how to handle message $other at this stage: $stage")))
  }
}
