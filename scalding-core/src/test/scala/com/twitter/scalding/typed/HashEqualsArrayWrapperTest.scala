package com.twitter.scalding.typed

import org.scalacheck.{ Arbitrary, Prop }
import org.scalatest.PropSpec
import org.scalatest.prop.{ Checkers, PropertyChecks }

object HashArrayEqualsWrapperLaws {

  def check2[T](ordToTest: Ordering[HashEqualsArrayWrapper[T]])(implicit ord: Ordering[T], arb: Arbitrary[Array[T]]): Prop =

    Prop.forAll { (left: Array[T], right: Array[T]) =>

      val leftWrapped = HashEqualsArrayWrapper.wrap(left)
      val rightWrapped = HashEqualsArrayWrapper.wrap(right)

      import scala.Ordering.Implicits.seqDerivedOrdering

      val slowOrd: Ordering[Seq[T]] = seqDerivedOrdering[Seq, T](ord)

      val cmp = ordToTest.compare(leftWrapped, rightWrapped)

      val lenCmp = java.lang.Integer.compare(leftWrapped.wrapped.length, rightWrapped.wrapped.length)
      if (lenCmp != 0) {
        cmp.signum == lenCmp.signum
      } else {
        cmp.signum == slowOrd.compare(leftWrapped.wrapped.toSeq, rightWrapped.wrapped.toSeq).signum
      }
    }

  def check[T](ordToTest: Ordering[Array[T]])(implicit ord: Ordering[T], arb: Arbitrary[Array[T]]): Prop =

    Prop.forAll { (left: Array[T], right: Array[T]) =>
      import scala.Ordering.Implicits.seqDerivedOrdering

      val slowOrd: Ordering[Seq[T]] = seqDerivedOrdering[Seq, T](ord)

      val cmp = ordToTest.compare(left, right)

      val lenCmp = java.lang.Integer.compare(left.length, right.length)
      if (lenCmp != 0) {
        cmp.signum == lenCmp.signum
      } else {
        cmp.signum == slowOrd.compare(left.toSeq, right.toSeq).signum
      }
    }
}

class HashArrayEqualsWrapperTest extends PropSpec with PropertyChecks with Checkers {

  property("Specialized orderings obey all laws for Arrays") {
    check(HashArrayEqualsWrapperLaws.check(HashEqualsArrayWrapper.longArrayOrd))
    check(HashArrayEqualsWrapperLaws.check(HashEqualsArrayWrapper.intArrayOrd))
    check(HashArrayEqualsWrapperLaws.check(HashEqualsArrayWrapper.shortArrayOrd))
    check(HashArrayEqualsWrapperLaws.check(HashEqualsArrayWrapper.charArrayOrd))
    check(HashArrayEqualsWrapperLaws.check(HashEqualsArrayWrapper.byteArrayOrd))
    check(HashArrayEqualsWrapperLaws.check(HashEqualsArrayWrapper.booleanArrayOrd))
    check(HashArrayEqualsWrapperLaws.check(HashEqualsArrayWrapper.floatArrayOrd))
    check(HashArrayEqualsWrapperLaws.check(HashEqualsArrayWrapper.doubleArrayOrd))
  }

  property("Specialized orderings obey all laws for wrapped Arrays") {
    check(HashArrayEqualsWrapperLaws.check2(HashEqualsArrayWrapper.hashEqualsLongOrdering))
    check(HashArrayEqualsWrapperLaws.check2(HashEqualsArrayWrapper.hashEqualsIntOrdering))
    check(HashArrayEqualsWrapperLaws.check2(HashEqualsArrayWrapper.hashEqualsShortOrdering))
    check(HashArrayEqualsWrapperLaws.check2(HashEqualsArrayWrapper.hashEqualsCharOrdering))
    check(HashArrayEqualsWrapperLaws.check2(HashEqualsArrayWrapper.hashEqualsByteOrdering))
    check(HashArrayEqualsWrapperLaws.check2(HashEqualsArrayWrapper.hashEqualsBooleanOrdering))
    check(HashArrayEqualsWrapperLaws.check2(HashEqualsArrayWrapper.hashEqualsFloatOrdering))
    check(HashArrayEqualsWrapperLaws.check2(HashEqualsArrayWrapper.hashEqualsDoubleOrdering))
  }

}