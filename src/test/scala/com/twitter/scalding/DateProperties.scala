package com.twitter.scalding

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._

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

  property("Shifting DateRanges breaks containment") = forAll { (dr: DateRange, r: Duration) =>
    val newDr = dr + r
    !newDr.contains(dr) || (newDr == dr)
  }

  property("Arithmetic works as expected") = forAll { (dr: DateRange, r: Duration) =>
    (dr + r) - r == dr &&
      (dr.start + r) - r == dr.start
  }
  property("fromMillisecs toMillisecs") = forAll { (unsafems: Long) =>
    val ms = unsafems/50
    val hours = ms/AbsoluteDuration.HOUR_IN_MS
    (Int.MinValue <= hours && hours <= Int.MaxValue) ==>
      (AbsoluteDuration.fromMillisecs(ms).toMillisecs == ms)
  }

  def asInt(b: Boolean) = if(b) 1 else 0

  property("Before/After works") = forAll { (dr: DateRange, rd: RichDate) =>
    (asInt(dr.contains(rd)) + asInt(dr.isBefore(rd)) + asInt(dr.isAfter(rd)) == 1) &&
      (dr.isBefore(dr.end + (dr.end - dr.start))) &&
      (dr.isAfter(dr.start - (dr.end - dr.start)))
  }

  def divDur(ad: AbsoluteDuration, div: Int) = AbsoluteDuration.fromMillisecs(ad.toMillisecs/div)

  property("each output is contained") = forAll { (dr: DateRange) =>
    val r = divDur(dr.end - dr.start, 10)
    dr.each(r).forall { dr.contains(_) }
  }

  property("Embiggen/extend always contains") = forAll { (dr: DateRange, d: Duration) =>
    dr.embiggen(d).contains(dr) &&
      dr.extend(d).contains(dr)
  }

  property("RichDate subtraction Roundtrip") = forAll { (utimestamp0: Long, utimestamp1: Long) =>
    val timestamp0 = utimestamp0/200
    val timestamp1 = utimestamp1/200
    val hours = (timestamp0 - timestamp1)/AbsoluteDuration.HOUR_IN_MS
    (Int.MinValue <= hours && hours <= Int.MaxValue) ==>
      ((RichDate(timestamp0) - RichDate(timestamp1)).toMillisecs == (timestamp0 - timestamp1))
  }
  property("Millisecs rt") = forAll { (ms: Int) =>
    Millisecs(ms).toMillisecs.toInt == ms
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
