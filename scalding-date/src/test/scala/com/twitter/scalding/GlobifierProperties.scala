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

import java.util.TimeZone

object GlobifierProperties extends Properties("Globifier Properties") {

  implicit def dateParser: DateParser = DateParser.default
  implicit def tz: TimeZone = TimeZone.getTimeZone("UTC")

  implicit val hourArb: Arbitrary[Hours] =
    Arbitrary { choose(0, 10000).map { Hours(_) } }

  implicit val dayArb: Arbitrary[Days] =
    Arbitrary { choose(0, 100).map { Days(_) } }

  implicit val yearArb: Arbitrary[Years] =
    Arbitrary { choose(0, 100).map { Years(_) } }

  implicit val richDateArb: Arbitrary[RichDate] = Arbitrary {
    for (v <- choose(0L, 1L << 32)) yield RichDate(v)
  }

  lazy val globifierOps = GlobifierOps()

  def testHrDr(dr: DateRange): Boolean = {
    val resultantDR = globifierOps.hourlyRtGlobifier(dr)
    val resultantWithNormalized = globifierOps.hourlyRtGlobifier(globifierOps.normalizeHrDr(dr))

    val res = globifierOps.normalizeHrDr(dr) == globifierOps.normalizeHrDr(resultantDR) &&
      globifierOps.normalizeHrDr(dr) == globifierOps.normalizeHrDr(resultantWithNormalized)

    if (!res) {
      println("Input dr: " + dr)
      println("resulting dr: " + resultantDR)
      println("resulting dr with pre-normalize: " + resultantWithNormalized)
    }
    res
  }

  // Laws to ensure we can round trip through the hour patterned globifier
  property("HR Globifier with hour deltas RT's") = forAll { (rndRD: RichDate, delta: Hours) =>
    val rd: RichDate = Hours(1).addTo(Hours(1).floorOf(rndRD))
    val dr = DateRange(rndRD, rd + delta)
    testHrDr(dr)
  }
  property("HR Globifier with Day deltas RT's") = forAll { (rndRD: RichDate, delta: Days) =>
    val rd: RichDate = Days(1).addTo(Days(1).floorOf(rndRD))
    val dr = DateRange(rndRD, rd + delta)
    testHrDr(dr)
  }

  property("HR Globifier with Year deltas RT's") = forAll { (rndRD: RichDate, delta: Years) =>
    val rd: RichDate = Years(1).addTo(Years(1).floorOf(rndRD))
    val dr = DateRange(rndRD, rd + delta)
    testHrDr(dr)
  }

  def testDayDr(dr: DateRange): Boolean = {
    val resultantDR = globifierOps.dailyRtGlobifier(dr)
    val resultantWithNormalized = globifierOps.dailyRtGlobifier(globifierOps.normalizeDayDr(dr))

    val res = globifierOps.normalizeDayDr(dr) == globifierOps.normalizeDayDr(resultantDR) &&
      globifierOps.normalizeDayDr(dr) == globifierOps.normalizeDayDr(resultantWithNormalized)

    if (!res) {
      println("Input dr: " + dr)
      println("resulting dr: " + resultantDR)
      println("resulting dr with pre-normalize: " + resultantWithNormalized)
    }
    res
  }
  // Laws to ensure we can round trip through the day patterned globifier
  property("Day Globifier with hour deltas RT's") = forAll { (rndRD: RichDate, delta: Hours) =>
    val rd: RichDate = Hours(1).addTo(Hours(1).floorOf(rndRD))
    val dr = DateRange(rndRD, rd + delta)
    testDayDr(dr)
  }
  property("Day Globifier with Day deltas RT's") = forAll { (rndRD: RichDate, delta: Days) =>
    val rd: RichDate = Days(1).addTo(Days(1).floorOf(rndRD))
    val dr = DateRange(rndRD, rd + delta)
    testDayDr(dr)
  }

  property("Day Globifier with Year deltas RT's") = forAll { (rndRD: RichDate, delta: Years) =>
    val rd: RichDate = Years(1).addTo(Years(1).floorOf(rndRD))
    val dr = DateRange(rndRD, rd + delta)
    testDayDr(dr)
  }
}
