package futuresql.main

import play.api.libs.iteratee._
import scala.concurrent.{ExecutionContext, Future}

/**
 * @author Bryce Anderson
 *         Created on 7/25/13
 */

// Should receive a buffer which is expecting more results
class QueryResult(enum: Future[Enumerator[RowIterator]])
                     (implicit ec: ExecutionContext) {

  def enumerate: Enumerator[RowIterator] = new Enumerator[RowIterator] {
    def apply[A](i: Iteratee[RowIterator, A]): Future[Iteratee[RowIterator, A]] =  enum.flatMap(_ |>> i )
  }
}
