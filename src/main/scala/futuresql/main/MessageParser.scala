package futuresql.main

import futuresql.nio.AsyncReadBuffer
import scala.concurrent.Future
import futuresql.postgres.PostgresMessage

/**
 * @author Bryce Anderson
 *         Created on 7/28/13
 */
trait MessageParser {
  def parseBuffer(buff: AsyncReadBuffer): Future[Message]
}
