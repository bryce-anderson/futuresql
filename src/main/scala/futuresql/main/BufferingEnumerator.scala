package futuresql.main

import play.api.libs.iteratee._
import scala.concurrent.{ExecutionContext, Promise, Future}
import scala.collection.mutable.ListBuffer
import scala.util.Success

/**
 * @author Bryce Anderson
 *         Created on 7/27/13
 */

object BufferingEnumerator {
  def apply[A](implicit ec: ExecutionContext): (Input[A] => Boolean, BufferingEnumerator[A]) = {
    val enum = new BufferingEnumerator[A]()
    (enum.feedInput(_), enum)
  }
}

class BufferingEnumerator[A] private[futuresql](implicit ec: ExecutionContext) extends Enumerator[A] {

  private val buffer = new ListBuffer[Input[A]]

  private var cont: Input[A] => Unit = { a => buffer += a }

  // Feed in the data, and return if it is closed or not
  private[BufferingEnumerator] def feedInput(in: Input[A]): Boolean = buffer.synchronized {
    if(cont != null) {
      cont(in)
      false
    } else true
  }


  def apply[B](i: Iteratee[A, B]): Future[Iteratee[A, B]] = {
    val p = Promise[Iteratee[A, B]]

    def itFolder(i: Iteratee[A, B], input: Input[A])(step: Step[A, B]) = Future.successful(input match {
      case Input.EOF =>
        p.complete(Success(i))
        cont = null
        i
      case input => step match {
        case Step.Cont(f) => f(input)
        case _ => // Finished.
          cont = null
          p.complete(Success(i))
          i
      }
    })

    def foldGenerator(i: Future[Iteratee[A, B]])(in: Input[A]): Unit = buffer.synchronized {
      val fi = i.flatMap( i =>  i.fold(itFolder(i, in)) )
      cont = foldGenerator(fi)
    }

    // Clean out the buffer and set the continuation
    buffer.synchronized {
      val fit = buffer.result().foldLeft(Future.successful(i)){ (i, input) =>
        i.flatMap ( i => i.fold(itFolder(i, input)))
      }
      buffer.clear()

      cont = foldGenerator(fit)
    }

    p.future
  }
}
