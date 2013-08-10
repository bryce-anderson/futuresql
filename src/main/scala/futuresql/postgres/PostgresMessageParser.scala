package futuresql.postgres

import futuresql.main.util.BufferUtils
import BufferUtils._
import scala.Some
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable.ListBuffer
import futuresql.nio.AsyncReadBuffer
import futuresql.main.MessageParser

/**
 * @author Bryce Anderson
 *         Created on 7/22/13
 */

class PostgresMessageParser(implicit ec: ExecutionContext) extends MessageParser {

  def parseBuffer(buff: AsyncReadBuffer): Future[PostgresMessage] = {
    buff.getChar.flatMap {
      case MessageCodes.DataRow => parseDataRow(buff)
      case MessageCodes.EmptyQueryResponse => parseEmptyQueryResponse(buff)
      case MessageCodes.ErrorResponse => parseError(buff)
      case MessageCodes.ParseComplete => parseParseComplete(buff)
      case MessageCodes.Authorize => parseAuthorize(buff)
      case MessageCodes.ParameterStatus => parseParameterStatus(buff)
      case MessageCodes.BackendKeyData => parseBackendKeyData(buff)
      case MessageCodes.ReadyForQuery => parseReadyForQuery(buff)
      case MessageCodes.RowDescription => parseRowDescription(buff)
      case MessageCodes.CommandComplete => parseCommandComplete(buff)
      case MessageCodes.NoticeResponse => parseNoticeResponse(buff)
      case MessageCodes.BindComplete => parseBindComplete(buff)

      case code => buff.getNextFullBuffer().flatMap { buff =>
        val str = new StringBuilder
        str.append(s"Cannot parse message with code: $code, ${code.toByte}\nRemaining: ")
        while(buff.remaining() > 0) str.append(buff.get().toChar)
        parseError(str.result)
      }
    }
      .map { m => println("Read message: " + m); m }
  }

  private def parseBindComplete(buff: AsyncReadBuffer): Future[BindComplete.type] = buff.getInt().map { i =>
    if(i != 4) sys.error("Wrong size: " + i)
    BindComplete
  }

  private def parseNoticeResponse(buff: AsyncReadBuffer): Future[NoticeResponse] = {
    buff.getNextFullBuffer().map { buff =>
      val mtype = buff.get()
      val msg = parseString(buff)
      NoticeResponse(mtype, msg)
    }
  }

  private def parseParseComplete(buff: AsyncReadBuffer): Future[ParseComplete.type] = buff.getInt() flatMap { size =>
    if (size != 4) Future.failed(new Exception("Wrong size for ParseComplete message: " + size))
    else Future.successful(ParseComplete)
  }

  private def parseError(buff: AsyncReadBuffer): Future[ErrorResponse] = buff.getNextFullBuffer().map { buff =>
    val code = buff.get()
    val strArray = new Array[Byte](buff.remaining() - 1)
    buff.get(strArray)
    ErrorResponse(new String(strArray), code)
  }

  private def parseEmptyQueryResponse(buff: AsyncReadBuffer): Future[PostgresMessage] = buff.getInt() map { i =>
    if (i != 4 ) throw error(s"EmptyQueryResponse had wrong size: $i. Required: 4.")
    EmptyQueryResponse
  }

  private def parseAuthorize(abuff: AsyncReadBuffer): Future[Auth] = {
    abuff.getBuffer(8) flatMap { buff =>
      val size = buff.getInt
      val authType = buff.getInt
      if (size == 8 && authType == 0) {
        Future.successful(AuthOK)
      } else if (size == 12 && authType == 5) {
        abuff.getBytes(4) map { salt =>
          AuthMD5(salt)
        }

      } else parseError(s"Unknown authentication message: Size=$size, Code=$authType")
    }
  }

  private def parseParameterStatus(buff: AsyncReadBuffer): Future[ParameterStatus] = buff.getNextFullBuffer() map { buff =>
    val param = new StringBuilder
    val value = new StringBuilder
    var chr = '\0'
    while({chr = buff.get().toChar; chr != '\0'})  param.append(chr)
    while({chr = buff.get().toChar; chr != '\0'})  value.append(chr)

    ParameterStatus(param.result(), value.result())
  }

  private def parseBackendKeyData(abuff: AsyncReadBuffer): Future[BackendKeyData] = abuff.getInt flatMap { size =>
    if (size != 12) parseError(s"Malformed BackendKeyData. Wrong size: $size. Should be 12.")
    else abuff.getBuffer(8) map { buff =>
      val id = buff.getInt
      val key = buff.getInt
      BackendKeyData(id, key)
    }
  }

  private def parseReadyForQuery(buff: AsyncReadBuffer): Future[ReadyForQuery] = buff.getBuffer(5) map { buff =>
    val size = buff.getInt
    if (size != 5) sys.error(s"Malformed Backend Status message: Size: $size. Must be 5")
    buff.get().toChar match {
      case 'I' => ReadyForQuery(Idle)
      case 'T' => ReadyForQuery(Transaction)
      case 'E' => ReadyForQuery(FailedTrans)
      case e => throw error(s"Malformed Backend Status message: Unknown ready status: $e")
    }
  }

  private def parseRowDescription(buff: AsyncReadBuffer): Future[RowDescription] = {
    buff.getNextFullBuffer map { buff =>
      val fields = buff.getShort
      val rows = new ListBuffer[Column]
      0 until fields foreach { i =>
        val c = Column(BufferUtils.parseString(buff),
        {
          val tableid = buff.getInt()
          val colid = buff.getShort
          if (tableid == 0 && colid == 0) None
          else Some(ColumnID(tableid, colid))
        },
        buff.getInt,      // Dtype
        buff.getShort,    // Dtype size
        buff.getInt,      // Dtype mod
        buff.getShort match { // Format is string or binary
          case 1 => true
          case 0 => false
          case e => throw error(s"Invalid format. Code: $e")
        }
        )
        rows.append(c)
      }
      RowDescription(rows.result())
    }
  }

  private def parseDataRow(buff: AsyncReadBuffer): Future[DataRow] = {
    println("Parsing Data Row")
    buff.getNextFullBuffer() map { buff =>
      val columns = buff.getShort()
      val data = new Array[Array[Byte]](columns)
      0 until columns foreach { i =>
        val size = buff.getInt()
        if (size < -1) throw error("Recieved invalid size: " + size)
        else if (size == -1 || size == 0) { // Null result
          data(i) = new Array[Byte](0)
        } else {
          data(i) = new Array[Byte](size)
          buff.get(data(i))
        }
      }

      DataRow(data)
    }
  }

  private def parseCommandComplete(buff: AsyncReadBuffer): Future[CommandComplete] = {
    println("Parsing CommandComplete")
    buff.getNextFullBuffer() map { buff =>
      CommandComplete(parseString(buff))
    }
  }


  private def parseError(msg: String) = Future.failed[Nothing](error(msg))
  private def error(msg: String) = new Exception(msg)

}