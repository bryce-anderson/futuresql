package futuresql.postgres

/**
 * @author Bryce Anderson
 *         Created on 7/24/13
 */

object MessageCodes {
  val ErrorResponse       = 'E'
  val Authorize           = 'R'
  val PasswdMessage       = 'p'
  val ParameterStatus     = 'S'
  val ReadyForQuery       = 'Z'
  val BackendKeyData      = 'K'
  val SimpleQuery         = 'Q'
  val RowDescription      = 'T'
  val CommandComplete     = 'C'
  val DataRow             = 'D'
  val Terminate           = 'X'
  val Parse               = 'P'
  val ParseComplete       = '1'
  val EmptyQueryResponse  = 'I'
  val NoticeResponse      = 'N'
}