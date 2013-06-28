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

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._

import scala.util.control.Exception.allCatch
import AbsoluteDuration.fromMillisecs

object DateProperties extends Properties("Date Properties") {

  implicit val durationArb: Arbitrary[Duration] =
    Arbitrary { choose(0, 10000).map { Millisecs(_) } }

  implicit val richDateArb: Arbitrary[RichDate] = Arbitrary {
    for(v <- choose(0L, 1L<<32)) yield RichDate(v)
  }
  implicit val dateRangeArb: Arbitrary[DateRange] = Arbitrary {
    for(v1 <- choose(0L, 1L<<33);
        v2 <- choose(v1, 1L<<33)) yield DateRange(RichDate(v1), RichDate(v2))
  }
  implicit val absdur: Arbitrary[AbsoluteDuration] =
    Arbitrary {
      implicitly[Arbitrary[Long]]
        .arbitrary
        // Ignore Longs that are too big to fit, and make sure we can add any random 3 together
        // Long.MaxValue / 1200 ms is the biggest that will fit, we divide by 3 to make sure
        // we can add three together in tests
        .map { ms => fromMillisecs(ms/(1200*3)) }
    }

  property("Shifting DateRanges breaks containment") = forAll { (dr: DateRange, r: Duration) =>
    val newDr = dr + r
    !newDr.contains(dr) || (newDr == dr)
  }

  property("Arithmetic works as expected") = forAll { (dr: DateRange, r: Duration) =>
    (dr + r) - r == dr &&
      (dr.start + r) - r == dr.start
  }
  property("fromMillisecs toMillisecs") = forAll { (ad: AbsoluteDuration) =>
    val ms = ad.toMillisecs
    (fromMillisecs(ms) == ad)
  }

  def asInt(b: Boolean) = if(b) 1 else 0

  property("Before/After works") = forAll { (dr: DateRange, rd: RichDate) =>
    (asInt(dr.contains(rd)) + asInt(dr.isBefore(rd)) + asInt(dr.isAfter(rd)) == 1) &&
      (dr.isBefore(dr.end + (dr.end - dr.start))) &&
      (dr.isAfter(dr.start - (dr.end - dr.start)))
  }

  def divDur(ad: AbsoluteDuration, div: Int) = fromMillisecs(ad.toMillisecs/div)

  property("each output is contained") = forAll { (dr: DateRange) =>
    val r = divDur(dr.end - dr.start, 10)
    dr.each(r).forall { dr.contains(_) }
  }

  property("Embiggen/extend always contains") = forAll { (dr: DateRange, d: Duration) =>
    dr.embiggen(d).contains(dr) &&
      dr.extend(d).contains(dr)
  }

  property("RichDate subtraction Roundtrip") = forAll { (timestamp0: Long, delta: AbsoluteDuration) =>
    val start = RichDate(timestamp0)
    val end = start + delta
    end - delta == start && (end - start) == delta
  }
  property("Millisecs rt") = forAll { (ms: Int) =>
    Millisecs(ms).toMillisecs.toInt == ms
  }

  property("AbsoluteDuration group properties") =
    forAll { (a: AbsoluteDuration, b: AbsoluteDuration, c: AbsoluteDuration) =>
      (a + b) - c == a + (b - c) &&
      (a + b) + c == a + (b + c) &&
      (a - a) == fromMillisecs(0) &&
      (b - b) == fromMillisecs(0) &&
      (c - c) == fromMillisecs(0) &&
      { b.toMillisecs == 0 || {
          // Don't divide by zero:
          val (d, rem) = (a/b)
          a == b * d + rem && (rem.toMillisecs.abs < b.toMillisecs.abs)
        }
      }
    }

  def toRegex(glob: String) = (glob.flatMap { c => if(c == '*') ".*" else c.toString }).r

  def matches(l: List[String], arg: String): Int = l
    .map { toRegex _ }
    .map { _.findFirstMatchIn(arg).map { _ => 1 }.getOrElse(0) }
    .sum

  // Make sure globifier always contains:
  val pattern = "%1$tY/%1$tm/%1$td/%1$tH"
  val glob = Globifier(pattern)(DateOps.UTC)
  property("Globifying produces matching patterns") = forAll { (dr: DateRange) =>
    val globbed = glob.globify(dr)
    // Brute force
    dr.each(Hours(1)).map { _.start.format(pattern)(DateOps.UTC) }
      .forall { matches(globbed, _) == 1 }
  }
}
