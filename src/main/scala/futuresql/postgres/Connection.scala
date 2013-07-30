package futuresql.postgres


import scala.concurrent.{Await, Promise, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.collection.mutable.ListBuffer

import java.nio.channels.{CompletionHandler, AsynchronousSocketChannel}
import java.net.InetSocketAddress
import java.util.concurrent.ExecutionException
import java.io.IOException

import play.api.libs.iteratee.{Input, Enumerator}

import futuresql.nio.{AsyncWriteBuffer, AsyncReadBuffer}
import futuresql.main.{BufferingEnumerator, AsyncMessageBuffer, Message, RowIterator}

/**
 * @author Bryce Anderson
 *         Created on 7/25/13
 */


object Connection {
  def newConnection(user: String, passwd: String, address: String, port: Int, db: String)
                   (recycle: Connection => Any = c => println(s"Recycling connection $c") )
                   (onError: (Connection, Throwable) => Any = (_, _) => Unit)(implicit ec: ExecutionContext) = {

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
    val login = Login(user, passwd, db)
    new Connection(login) { self =>
      def newChannel(): AsynchronousSocketChannel = simpleFactory()

      def onDeath(conn: Connection, t: Throwable): Any = onError(conn,t)

      def recycleConnection(conn: Connection): Any = recycle(conn)

      def log(msg: String) = println(s"Connection $self: DEBUG: $msg")
    }
  }
}


private[postgres] abstract class Connection(login: Login)(implicit ec: ExecutionContext) { self =>

  def newChannel(): AsynchronousSocketChannel

  def onDeath(conn: Connection, t: Throwable): Any

  def recycleConnection(conn: Connection): Any

  def log(msg: String)


  private var _isClosed = false

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

  def onParamStatus(ps: ParameterStatus) { }

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

  private def recycleConnection() {
    messagebuff.getMessage().onComplete {
      case Success(ReadyForQuery(_)) => recycleConnection(self)
      case Success(m: Message) =>
        log("Cleaned messaage:" + m)
        recycleConnection()

      case Failure(t) => onDeath(self, t)
    }
  }

  private def cancelQuery() {
    log("Canceling Query.")
    try {
      val tempChannel = newChannel()
      tempChannel.write(CancelRequest(keyData).toBuffer, null: Null, new CompletionHandler[Integer, Null] {
        def completed(result: Integer, attachment: Null) {
          tempChannel.close()
        }
        def failed(exc: Throwable, attachment: Null) {}
      })
    } catch {
      case t: Throwable => // Don't care
    } finally  recycleConnection()
  }

  private def feedRows(desc: RowDescription, pusher: Input[RowIterator] => Boolean) {
    messagebuff.getMessage().onComplete {
      case Success(m: DataRow) =>
        if (!pusher(Input.El(new RowIterator(desc, m)))) feedRows(desc, pusher)
        else cancelQuery()

      case Success(CommandComplete(msg)) =>
        log("Command complete: " + msg)
        pusher(Input.EOF)
        recycleConnection()

      case Success(m: Message) =>
        log("Found unexpected message: " + m)
        sys.error("Don't know how to respond to message: " + m)

      case Failure(t) =>  throw new Exception("Failed to parse.", t)
    }
  }

  def query(query: String): Future[Enumerator[RowIterator]] = {
    val q = SimpleQuery(query)
    val p = Promise[Enumerator[RowIterator]]
    writebuff.writeBuffer(q.toBuffer).onComplete {
      case Success(_) =>
        messagebuff.getMessage().map {
          case EmptyQueryResponse =>
            p.complete(Success(Enumerator()))
            recycleConnection()

          case CommandComplete(msg) =>
            log("Command complete: " + msg)
            recycleConnection()
            p.complete(Success(Enumerator()))

          case desc: RowDescription =>  // Getting data. Start to read
            val (pusher, enum) = BufferingEnumerator.get[RowIterator]
            feedRows(desc, pusher)
            p.complete(Success(enum))

          case other => log("Found unexpected message: " + other); handleUnexpected(other, p, "SimpleQuery")
        }

      case Failure(t) => p.complete(Failure(t))
    }

    p.future
  }

  def query(query: String, params: Any*): Future[Enumerator[RowIterator]] = {
    ???
  }

  def isClosed() = _isClosed

  def close() {
    _isClosed = true
    log("Closing Channel")
    writebuff.writeBuffer(Terminate.toBuffer).flatMap( _ => messagebuff.close())
                .onComplete( _ => channel.close())
  }

  def handleUnexpected(msg: Message, p: Promise[_], stage: String) = msg match {
    case e: ErrorResponse =>
      val ex = new Exception(s"Failed query in stage $stage with error code '${e.errorcode}'. Msg: ${e.msg}")
      p.complete(Failure(ex))

    case other => p.complete(Failure(new Exception(s"Don't know how to handle message $other at this stage: $stage")))
  }
}