package com.twitter.scalding.mathematics

import org.specs._

class AbstractAlgebraTest extends Specification {
  noDetailedDiffs()
  "A Monoid should be able to sum" in {
    val monoid = implicitly[Monoid[Int]]
    val list = List(1,5,6,6,4,5)
    list.sum must be_==(monoid.sum(list))
  }
  "A Ring should be able to product" in {
    val ring = implicitly[Ring[Int]]
    val list = List(1,5,6,6,4,5)
    list.product must be_==(ring.product(list))
  }
}
