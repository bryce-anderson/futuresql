package futuresql.postgres

import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.collection.mutable
import play.api.libs.iteratee.Enumerator
import scala.util.{Random, Success}
import futuresql.main.{QueryResult, RowIterator}
import java.nio.channels.{AsynchronousSocketChannel, AsynchronousCloseException}
import java.net.InetSocketAddress
import java.util.concurrent.ExecutionException
import java.io.IOException
import futuresql.postgres.types.QueryParam
import futuresql.postgres.Connection

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
    if(isClosed()) {
      conn.close()
    }else if (!queryQueue.isEmpty) {
      queryQueue.dequeue().complete(Success(conn))
    } else {
      connectionQueue += conn
    }
  }

  protected def connectionError(conn: Connection, t: Throwable): Unit = if (!isClosed) {
    println("DEBUG: Caught error: " + t + ". Restarting connection.")
    //t.printStackTrace()
    //recycleConnection(conn)
    conn.close()
    recycleConnection(makeConnection(-1))

  }

  def isClosed() = _isClosed

  def close(): Unit = lock.synchronized {
    _isClosed = true
    queryQueue.foreach { p => p.tryFailure(new AsynchronousCloseException)}
    connectionQueue.foreach( c => c.close())
    queryQueue.clear()
    connectionQueue.clear()
  }

  private def runQuery(f: Connection => Future[Enumerator[RowIterator]]) = lock.synchronized {
    if(_isClosed) sys.error("Attempted to submit query to closed connection pool.")
    if(!connectionQueue.isEmpty) {
      val conn = connectionQueue.dequeue()
      new QueryResult(f(conn))
    } else {
      val p = Promise[Connection]
      queryQueue += p
      new QueryResult(p.future.flatMap(conn => f(conn)))
    }
  }

  def query(query: String): QueryResult = runQuery( conn => conn.query(query))

  def preparedQuery(query: String, params: QueryParam*): QueryResult =
    runQuery( conn => conn.preparedQuery(query, params))

}
