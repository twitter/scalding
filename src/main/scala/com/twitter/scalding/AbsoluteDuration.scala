/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding

import java.util.Calendar

import scala.annotation.tailrec

/*
 * These are reasonably indepedendent of calendars (or we will pretend)
 */
object AbsoluteDuration extends java.io.Serializable {
  def max(a : AbsoluteDuration, b : AbsoluteDuration) = if(a > b) a else b

  type TimeCons = ((Int) => AbsoluteDuration, Int)

  val SEC_IN_MS = 1000
  val MIN_IN_MS = 60 * SEC_IN_MS
  val HOUR_IN_MS = 60 * MIN_IN_MS
  val UTC_UNITS = List[TimeCons]((Hours,HOUR_IN_MS),
    (Minutes,MIN_IN_MS),
    (Seconds,SEC_IN_MS),
    (Millisecs,1)).reverse

  def exact(fnms: TimeCons): (Long) => Option[AbsoluteDuration] = { ms: Long =>
    if( ms % fnms._2 == 0 ) {
      Some(fnms._1( (ms / fnms._2).toInt ))
    }
    else {
      None
    }
  }

  def fromMillisecs(diffInMs: Long): AbsoluteDuration = fromMillisecs(diffInMs, UTC_UNITS, Nil)

  @tailrec
  private def fromMillisecs(diffInMs: Long, units: List[TimeCons], acc: List[AbsoluteDuration]):
    AbsoluteDuration = {

    if(diffInMs == 0L) {
      //We are done:
      acc match {
        case Nil => units.head._1(0)
        case (h :: Nil) => h
        case _ => AbsoluteDurationList(acc)
      }
    }
    else {
      units match {
        case (tc0 :: tc1 :: tail) => {
          //Only get as many as the next guy can't get:
          val nextSize = tc1._2
          val thisDiff = diffInMs % nextSize // Keep only this amount of millis for this unit
          val theseUnits = thisDiff / tc0._2
          val (newDiff, newAcc) = if (theseUnits != 0L) {
            val dur = tc0._1(theseUnits.toInt)
            (diffInMs - dur.toMillisecs, dur::acc)
          }
          else {
            (diffInMs, acc)
          }
          fromMillisecs(newDiff, (tc1 :: tail), newAcc)
        }
        case (tc :: Nil) => {
          // We can't go any further, try to jam the rest into this unit:
          val (fn, cnt) = tc
          val theseUnits = diffInMs / cnt
          require((theseUnits <= Int.MaxValue) && (theseUnits >= Int.MinValue),
            "diff not representable in an Int")
          val thisPart = fn(theseUnits.toInt)
          if (acc.isEmpty)
            thisPart
          else
            AbsoluteDurationList(thisPart :: acc)
        }
        case Nil => {
          // These are left over millisecs, but should be unreachable
          sys.error("this is only reachable if units is passed with a length == 0, which should never happen")
        }
      }
    }
  }
}

sealed trait AbsoluteDuration extends Duration with Ordered[AbsoluteDuration] {
  def toSeconds : Double = {
    calField match {
      case Calendar.MILLISECOND => count / 1000.0
      case Calendar.SECOND => count.toDouble
      case Calendar.MINUTE => count * 60.0
      case Calendar.HOUR => count * 60.0 * 60.0
    }
  }
  def toMillisecs : Long = {
    calField match {
      case Calendar.MILLISECOND => count.toLong
      case Calendar.SECOND => count.toLong * 1000L
      case Calendar.MINUTE => count.toLong * 1000L * 60L
      case Calendar.HOUR => count.toLong * 1000L * 60L * 60L
    }
  }
  def compare(that : AbsoluteDuration) : Int = {
    this.toMillisecs.compareTo(that.toMillisecs)
  }
  def +(that : AbsoluteDuration) = {
    AbsoluteDuration.fromMillisecs(this.toMillisecs + that.toMillisecs)
  }
  override def equals(eq: Any): Boolean = {
    eq match {
      case eqo: AbsoluteDuration => (eqo.toMillisecs) == this.toMillisecs
      case _ => false
    }
  }
  override def hashCode: Int = toMillisecs.hashCode
}

case class Millisecs(cnt : Int) extends Duration(Calendar.MILLISECOND, cnt, DateOps.UTC)
  with AbsoluteDuration

case class Seconds(cnt : Int) extends Duration(Calendar.SECOND, cnt, DateOps.UTC)
  with AbsoluteDuration

case class Minutes(cnt : Int) extends Duration(Calendar.MINUTE, cnt, DateOps.UTC)
  with AbsoluteDuration

case class Hours(cnt : Int) extends Duration(Calendar.HOUR, cnt, DateOps.UTC)
  with AbsoluteDuration

case class AbsoluteDurationList(parts : List[AbsoluteDuration])
  extends AbstractDurationList[AbsoluteDuration](parts) with AbsoluteDuration {
  override def toSeconds = parts.map{ _.toSeconds }.sum
  override def toMillisecs : Long = parts.map{ _.toMillisecs }.sum
}
