package com.twitter.scalding.db

import org.scalacheck.Properties
import org.scalacheck.Prop._

object DBOptionsTest extends Properties("DBOptions") {
  property("password") = forAll { x: String =>
    ("Password toString should not be equal to x" |: Password(x).toString != x) &&
      ("Password toStr should be equal to x" |: Password(x).toStr == x)
  }
}
