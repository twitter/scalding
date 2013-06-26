package com.twitter.scalding.mathematics

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.scalding.TDsl._
import com.twitter.scalding._
import com.twitter.algebird.Monoid

object Matrix2 {  
	sealed abstract class Matrix2
	  (val sizeHint : SizeHint = NoClue) {
	  def +(that: Matrix2): Matrix2 = Sum(this, that)
	  def *(that:  Matrix2):  Matrix2 = Product(this, that)
	  val tpipe: TypedPipe[(Int, Int, Double)]
  
	}

	case class Product(val left: Matrix2, val right: Matrix2) extends Matrix2 {
	  override lazy val tpipe = left.tpipe
	  override val sizeHint = left.sizeHint * right.sizeHint
	}

	case class Sum(val left: Matrix2, val right: Matrix2) extends Matrix2 {
	  def toPipe()(implicit mon : Monoid[Double], ord: Ordering[(Int, Int)]) : TypedPipe[(Int,Int,Double)] = {
	    if (left.equals(right)) {
	      left.tpipe.map(v => (v._1, v._2, mon.plus(v._3, v._3)))
	    } else {
	      (left.tpipe ++ right.tpipe).groupBy(x => (x._1, x._2)).mapValueStream(vals => {
	        if (vals.size == 1) vals else {
	          val l = vals.next()
	          val r = vals.next()
	          val res = mon.plus(l._3, r._3)
	          if (res == mon.zero) Iterator() else Iterator((l._1, l._2, res))
	        }
	      }).toTypedPipe.map(y => y._2)
	      }
	    }
	  
	  override lazy val tpipe = toPipe()
	  override val sizeHint = left.sizeHint + right.sizeHint
	}

	case class Literal(val fields: Fields, val inPipe: Pipe, override val sizeHint : SizeHint) extends Matrix2 {
	  override lazy val tpipe = inPipe.toTypedPipe[(Int, Int, Double)](fields)
	}
	
	
}
