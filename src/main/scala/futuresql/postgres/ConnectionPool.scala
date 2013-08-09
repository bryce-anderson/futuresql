package futuresql.postgres

import scala.concurrent.{Promise, ExecutionContext}
import scala.collection.mutable
import play.api.libs.iteratee.Enumerator
import scala.util.{Random, Success}
import futuresql.main.{QueryResult, RowIterator}
import java.nio.channels.{AsynchronousSocketChannel, AsynchronousCloseException}
import java.net.InetSocketAddress
import java.util.concurrent.ExecutionException
import java.io.IOException

/**
 * @author Bryce Anderson
 *         Created on 7/27/13
 */
class ConnectionPool(user: String, passwd: String, address: String, port: Int, db: String, size: Int = 20, options: Iterable[String] = Nil)
                    (implicit ec: ExecutionContext) { pool =>

  private var _isClosed = false
  private val queryQueue = new mutable.Queue[Promise[Connection]]()
  private val connectionQueue = new mutable.Queue[Connection]()
  protected val lock = new AnyRef

  def log(msg: String, connection: Int): Unit = println(s"Connection $connection: DEBUG: $msg")

  private def makeConnection(num: Int) = {

    new Connection(Login(user, passwd, db)) {
      def log(msg: String) = pool.log(msg, num)

      def onDeath(conn: Connection, t: Throwable): Any = connectionError(conn, t)


      def newChannel(): AsynchronousSocketChannel = {
        try {
          val channel = AsynchronousSocketChannel.open()
          channel.connect(new InetSocketAddress(address, port)).get()
          channel
        } catch {
          case exec: ExecutionException => throw exec
          case io: IOException => throw io
        }
      }

      def recycleConnection(conn: Connection): Any = pool.recycleConnection(conn)
    }
  }

  // Build our connections
  0 until size foreach { i => connectionQueue += makeConnection(i) }

  protected def recycleConnection(conn: Connection): Unit = lock.synchronized {
    if (!queryQueue.isEmpty) {
      queryQueue.dequeue().complete(Success(conn))
    } else {
      connectionQueue += conn
    }
  }

  protected def connectionError(conn: Connection, t: Throwable): Unit = if (!isClosed) {
    println("DEBUG: Caught error: " + t + ". Restarting connection.")
    t.printStackTrace()
    recycleConnection(conn)
  }

  def isClosed() = _isClosed

  def close(): Unit = lock.synchronized {
    _isClosed = true
    queryQueue.foreach { p => p.tryFailure(new AsynchronousCloseException)}
    connectionQueue.foreach( c => c.close())
    queryQueue.clear()
    connectionQueue.clear()
  }


  def query(query: String): QueryResult = lock.synchronized {
    if(_isClosed) sys.error("Attempted to submit inQuery to closed connection pool.")
    if(!connectionQueue.isEmpty) {
      val conn = connectionQueue.dequeue()
      new QueryResult(conn.query(query))
    } else {
      val p = Promise[Connection]
      queryQueue += p
      new QueryResult(p.future.flatMap{ conn => conn.query(query)})
    }
  }

  def query(query: String, params: String*): QueryResult = lock.synchronized (???)

}
