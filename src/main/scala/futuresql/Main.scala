package futuresql

import scala.concurrent.Await
import scala.concurrent.duration._
import play.api.libs.iteratee.{Enumerator, Iteratee}
import futuresql.main.RowIterator

import com.typesafe.config.ConfigFactory

object Main {
  def main(args: Array[String]) {
    println("Hello world!")

    postgresPool()
    Thread.sleep(1000)
    println("Ending Test program.")
  }

  def postgresPool() {
    val conf = ConfigFactory.load()
    import concurrent.ExecutionContext.Implicits.global

    val updateQuery = """update users set name='cool' where id=2;"""
    val selectQuery = """select * from users;"""
    val binQuery = """select * from bintest;"""

    val pool = new postgres.ConnectionPool(conf.getString("db.username"),
                                           conf.getString("db.password"),
                                           conf.getString("db.address"),
                                           conf.getInt("db.port"),
                                           conf.getString("db.dbname"),
                                                  1)

    val enums = 0 until 1 map { _ =>
      pool.preparedQuery(selectQuery).enumerate
    } reduceLeft ( _ >>> _ )

    var count = 0
    val f = enums >>>
      Enumerator.eof |>>
      Iteratee.foreach[RowIterator]{ r => println(s"$count: Found Data: " + r.dataMap); count += 1}

    Await.result(f, 4.seconds)

    println(Await.result(pool.query(updateQuery).enumerate |>> Iteratee.foreach[RowIterator]( i => println("Found " + i)), 2.seconds))
    println(Await.result(pool.query(binQuery).enumerate |>> Iteratee.foreach[RowIterator]( i => println("Found " + i.dataMap)), 2.seconds))

    pool.close()
  }

}
