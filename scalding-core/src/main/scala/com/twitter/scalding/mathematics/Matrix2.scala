package com.twitter.scalding.mathematics

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.scalding.TDsl._
import com.twitter.scalding._
import com.twitter.algebird.{Monoid, Ring}

object Matrix2 {  
	sealed abstract class Matrix2
	  (val sizeHint : SizeHint = NoClue) {
	  def +(that: Matrix2): Matrix2 = Sum(this, that)
	  def *(that:  Matrix2):  Matrix2 = Product(this, that)
	  val tpipe: TypedPipe[(Int, Int, Double)]
	  def transpose : Matrix2 = Literal(tpipe.map(x => (x._2, x._1, x._3)), sizeHint)
	}

	case class Product(val left: Matrix2, val right: Matrix2) extends Matrix2 {
	  def toPipe()(implicit ring: Ring[Double], ord: Ordering[(Int, Int)]) : TypedPipe[(Int,Int,Double)] = {		
	    val one = left.tpipe.groupBy(x => x._2)
	    val two = right.tpipe.groupBy(x => x._1)
	    
	    one.join(two).mapValueStream(x => x.map(y => (y._1._1, y._2._2, ring.times(y._1._3, y._2._3)))).values.
	    groupBy(w => (w._1, w._2)).mapValueStream(s => Iterator(s.reduce((a,b) => (a._1, a._2, ring.plus(a._3, b._3))))).values

	  }
	  
	  override lazy val tpipe = toPipe()
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

	case class Literal(override val tpipe: TypedPipe[(Int, Int, Double)], override val sizeHint : SizeHint) extends Matrix2 {
	  def this(fields: Fields, inPipe: Pipe, sizeHint : SizeHint) = this(inPipe.toTypedPipe[(Int, Int, Double)](fields), sizeHint) 
	}
	
}
