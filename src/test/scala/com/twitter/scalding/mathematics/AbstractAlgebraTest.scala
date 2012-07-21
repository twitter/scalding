package com.twitter.scalding.mathematics

import org.specs._

class MonPlus[T](l : T)(implicit m : Monoid[T]) {
  def mplus(r : T) : T = m.plus(l, r)
}

class AbstractAlgebraTest extends Specification {
  noDetailedDiffs()
  // Allow x mplus y as monoid plus:
  implicit def plusOp[T : Monoid](l : T) = new MonPlus(l)
  "Plus should work" in {
    0 mplus 3 must be_==(3)
    4 mplus 3 must be_==(7)

    List(2,3) mplus List(1,2,3) must be_==(List(2,3,1,2,3))

    "hey" mplus "you" must be_==("heyyou")

    Map(1 -> 3) mplus Map(1 -> 4, 2 -> 5) must be_==(Map(1->7, 2->5))
    (1,2) mplus (3,4) must be_==((4,6))

    (Left("error") : Either[String,Int]) mplus (Right(1) : Either[String,Int]) must be_==(Left("error"))
    (Left("error") : Either[String,Int]) mplus (Left(" happened") : Either[String,Int]) must be_==(Left("error happened"))
    (Right(1) : Either[String,Int]) mplus (Left(" happened") : Either[String,Int]) must be_==(Left(" happened"))
    (Right(1) : Either[String,Int]) mplus (Right(5) : Either[String,Int]) must be_==(Right(6))
    // the below doesn't work, because the type inferred is Right[Noting, Int], and Monoid is an
    // invariant type
    //Right(1) mplus Right(5) must be_==(Right(6))
    implicitly[Monoid[Either[String,Int]]].plus(Right(1),Right(6)) must be_==(Right(7))
  }
  "SortedTakeList monoid should work" in {
    implicit val listMon = new SortedTakeListMonoid[Int](3)
    List[Int]() mplus ((0 until 5).toList) must be_==(List(0,1,2))
    List(-1) mplus ((0 until 5).toList) must be_==(List(-1,0,1))
  }
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
