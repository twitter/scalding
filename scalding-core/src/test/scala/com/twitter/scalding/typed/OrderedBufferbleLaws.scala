package com.twitter.scalding.typed

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._

object OrderedBufferableLaws extends Properties("OrderedBufferableLaws") {

  property("StringOrderedBufferable follows law1") = {
    implicit val strOrd = new StringOrderedBufferable
    forAll(OrderedBufferable.law1)
  }
  property("StringOrderedBufferable follows law2") = {
    implicit val strOrd = new StringOrderedBufferable
    forAll(OrderedBufferable.law2)
  }
  property("StringOrderedBufferable follows law3") = {
    implicit val strOrd = new StringOrderedBufferable
    forAll(OrderedBufferable.law3)
  }

  property("UnsignedLongCompare works") = forAll { (l1: Long, l2: Long) =>
    val cmp = OrderedBufferable.unsignedLongCompare(l1, l2)
    (l1 >= 0, l2 >= 0) match {
      case (true, true) => cmp == java.lang.Long.compare(l1, l2)
      case (true, false) => cmp < 0 // negative is bigger
      case (false, true) => cmp > 0
      case (false, false) => cmp == java.lang.Long.compare(l1 & Long.MaxValue, l2 & Long.MaxValue)
    }
  }
  property("UnsignedIntCompare works") = forAll { (l1: Int, l2: Int) =>
    val cmp = OrderedBufferable.unsignedIntCompare(l1, l2)
    (l1 >= 0, l2 >= 0) match {
      case (true, true) => cmp == java.lang.Integer.compare(l1, l2)
      case (true, false) => cmp < 0 // negative is bigger
      case (false, true) => cmp > 0
      case (false, false) => cmp == java.lang.Integer.compare(l1 & Int.MaxValue, l2 & Int.MaxValue)
    }
  }
  property("UnsignedByteCompare works") = forAll { (l1: Byte, l2: Byte) =>
    def clamp(i: Int) = if (i > 0) 1 else if (i < 0) -1 else 0
    val cmp = clamp(OrderedBufferable.unsignedByteCompare(l1, l2))
    (l1 >= 0, l2 >= 0) match {
      case (true, true) => cmp == clamp(java.lang.Byte.compare(l1, l2))
      case (true, false) => cmp < 0 // negative is bigger
      case (false, true) => cmp > 0
      // Convert to positive ints
      case (false, false) => cmp == java.lang.Integer.compare(l1 & Byte.MaxValue, l2 & Byte.MaxValue)
    }
  }
}

