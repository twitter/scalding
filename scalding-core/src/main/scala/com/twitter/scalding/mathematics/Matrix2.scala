package com.twitter.scalding.mathematics

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.scalding.TDsl._
import com.twitter.scalding._

object Matrix2 {
	sealed abstract class Matrix2[RowT, ColT, ValT]
	  (val sizeHint : SizeHint = NoClue) {
	  def +(that: Matrix2[RowT, ColT, ValT]):  Matrix2[RowT, ColT, ValT] = Sum(this, that)
	  def *(that:  Matrix2[RowT, ColT, ValT]):  Matrix2[RowT, ColT, ValT] = Product(this, that)
	  val tpipe: TypedPipe[(RowT, ColT, ValT)]
	}

	case class Product[RowT, ColT, ValT](val left :  Matrix2[RowT, ColT, ValT], val right :  Matrix2[RowT, ColT, ValT]) extends Matrix2[RowT, ColT, ValT] {
	  override lazy val tpipe = left.tpipe
	  override val sizeHint = left.sizeHint * right.sizeHint
	}

	case class Sum[RowT, ColT, ValT](val left:  Matrix2[RowT, ColT, ValT], val right: Matrix2[RowT, ColT, ValT]) extends Matrix2[RowT, ColT, ValT] {
	  override lazy val tpipe = left.tpipe
	  override val sizeHint = left.sizeHint + right.sizeHint
	}

	case class Literal[RowT, ColT, ValT](val fields: Fields, val inPipe: Pipe, override val sizeHint : SizeHint) extends Matrix2[RowT, ColT, ValT] {
	  override lazy val tpipe = inPipe.toTypedPipe[(RowT, ColT, ValT)](fields)
	}
}
