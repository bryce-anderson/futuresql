package futuresql.postgres

import java.nio.ByteBuffer
import futuresql.main.util.BufferUtils
import BufferUtils._
import java.security.MessageDigest
import futuresql.main.{WritableMessage, Message, RowIterator}
import futuresql.main.util.BufferUtils

/**
 * @author Bryce Anderson
 *         Created on 7/24/13
 */

sealed abstract class PostgresMessage(val code: Char) extends Message

sealed abstract class WritablePostgresMessage(code: Char) extends PostgresMessage(code) with WritableMessage

sealed abstract class Auth(size: Int, authtype: Int) extends PostgresMessage(MessageCodes.Authorize)

case object Terminate extends WritablePostgresMessage(MessageCodes.Terminate) {
  def toBuffer = {
    val buff = newBuff(5)
    buff.put(code.toByte)
    buff.putInt(4)
    buff.flip()
    buff
  }
}

case class NoticeResponse(mtype: Byte, msg: String) extends PostgresMessage(MessageCodes.NoticeResponse)

case object AuthOK extends Auth(8, 0)

case class AuthMD5(salt: Array[Byte]) extends Auth(12, 5)

case class ErrorResponse(msg: String, errorcode: Byte) extends WritablePostgresMessage(MessageCodes.ErrorResponse) {

  def toBuffer: ByteBuffer = {
    val buff = newBuff(msg.length + 7) //  msgcode: 1, size: 4, error code: 1, str, null char: 1
    putString(buff, msg)
    buff.flip()
    buff
  }
}

case class DataRow(columns: Array[Array[Byte]]) extends PostgresMessage(MessageCodes.DataRow) {
  override def toString(): String = {
    val buf = new StringBuilder()
    buf.append("DataRow(")
    buf.append(columns.map(new String(_)).mkString(", "))
    buf.append(")")
    buf.result()
  }
}

case class BackendKeyData(id: Int, key: Int) extends PostgresMessage(MessageCodes.BackendKeyData)

sealed trait BackEndStatus
case object Idle extends BackEndStatus
case object Transaction extends BackEndStatus
case object FailedTrans extends BackEndStatus

case class ReadyForQuery(status: BackEndStatus) extends PostgresMessage(MessageCodes.ReadyForQuery)

case class SimpleQuery(query: String) extends WritablePostgresMessage(MessageCodes.SimpleQuery) {
  def toBuffer = {
    val buff = newBuff(6 + query.length)
    buff.put(code.toByte)
    buff.putInt(5 + query.length)
    buff.put(query.getBytes)
    buff.put('\0'.toByte)
    buff.flip()
    buff
  }
}
case class Column(name: String,
                  column: Option[ColumnID],
                  dtype: Int,
                  typesize: Short,
                  typemod: Int,
                  isbinary: Boolean)

case class ColumnID(tableID: Int, column: Short)

case class RowDescription(columns: List[Column]) extends PostgresMessage(MessageCodes.RowDescription) { self =>
  def parseRow(row: DataRow) = new RowIterator(self, row)
}

case class Parse(statement: String, name: String, paramTypes: List[Int]) extends WritablePostgresMessage(MessageCodes.Parse) {
  def toBuffer: ByteBuffer = {
    val paramscount = paramTypes.length
    val size = 4 + (statement.length + 1) + (name.length + 1) + 2 + 4*paramscount // size argument
    val buff = newBuff(size + 1)

    buff.put(code.toByte)
    buff.putInt(size)
    putString(buff, name)
    putString(buff, statement)
    buff.putShort(paramscount.asInstanceOf[Short])
    paramTypes.foreach(buff.putInt(_))
    buff.flip()
    buff
  }
}

case object EmptyQueryResponse extends PostgresMessage(MessageCodes.EmptyQueryResponse)

case object ParseComplete extends PostgresMessage(MessageCodes.ParseComplete)

case class ParameterStatus(param: String, value: String) extends PostgresMessage(MessageCodes.ParameterStatus)

case class CommandComplete(msg: String) extends PostgresMessage(MessageCodes.CommandComplete)

case class PasswordMesssage(username: String, password: String)
                    extends WritablePostgresMessage(MessageCodes.PasswdMessage) {
  private val fullPassword = password + username + '\0'

  def toBuffer: ByteBuffer = {
    val buff = newBuff(5 + fullPassword.length)
    buff.put(MessageCodes.PasswdMessage.toByte)
    buff.putInt(5 + fullPassword.length)
    buff.put(fullPassword.getBytes)
    buff.flip()
    buff
  }
}

class MD5PasswordMessage(username: String, password: String, salt: Array[Byte])
  extends PasswordMesssage(username, password) {

  override def toBuffer: ByteBuffer = {

    val md =  MessageDigest.getInstance("MD5")
    md.update(password.getBytes)
    md.update(username.getBytes)

    val firstDigest = md.digest()
    firstDigest.foreach{ b => md.update("%02x".format(b).getBytes) }
    md.update(salt)

    val str = {
      val str = new StringBuilder
      str.append("md5")
      md.digest().foreach( b => str.append("%02x".format(b)))
      str.append('\0')
      str.result()
    }

    val buff = newBuff(5 + str.length)
    buff.put(MessageCodes.PasswdMessage.toByte)
    buff.putInt(4 + str.length)
    buff.put(str.getBytes)
    buff.flip()
    buff
  }
}

object MD5PasswordMessage {
  def apply(username: String, password: String, salt: Array[Byte]) =
    new MD5PasswordMessage(username, password, salt)
}

case class Login(username: String, pass: String, db: String, options: TraversableOnce[String] = Nil) {
  def initiationBuffer: ByteBuffer = {
    val buff = ByteBuffer.allocate(2048)

    buff.putInt(0) // Will be set later
    buff.putInt(196608)   // Spec. Protocol 3.0
    putString(buff, "user")
    putString(buff, username)
    if (db != null) {
      putString(buff, "database")
      putString(buff, db)
    }
    for (opt <- options) {
      putString(buff, "option")
      putString(buff, opt)
    }
    buff.put(0.toByte)  // Required
    val size = buff.position()
    buff.position(0)
    buff.putInt(size)
    buff.position(size)
    buff.flip()
    buff
  }

  def authBuffer(salt: Array[Byte]): ByteBuffer = {
    MD5PasswordMessage(username, pass, salt).toBuffer
  }
}

case class CancelRequest(secrets: BackendKeyData) {
  def toBuffer = {
    val buff = newBuff(16)
    buff.putInt(80877102)
    buff.putInt(secrets.id)
    buff.putInt(secrets.key)
    buff.flip()
    buff
  }
}