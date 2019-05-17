package com.twitter.scalding.typed.gen

import org.scalacheck.Gen

object StdGen {
  implicit val stringGen: Gen[String] = Gen.alphaStr

  implicit val charGen: Gen[Char] = Gen.alphaChar

  implicit val booleanGen: Gen[Boolean] = Gen.oneOf(true, false)

  implicit val unitGen: Gen[Unit] = Gen.const(())

  implicit val byteGen: Gen[Byte] = Gen.chooseNum(Byte.MinValue, Byte.MaxValue)

  implicit val shortGen: Gen[Short] = Gen.chooseNum(Short.MinValue, Short.MaxValue)

  implicit val intGen: Gen[Int] = Gen.chooseNum(Int.MinValue, Int.MaxValue)

  implicit val longGen: Gen[Long] = Gen.chooseNum(Long.MinValue, Long.MaxValue)

  implicit val floatGen: Gen[Float] = Gen.chooseNum(Float.MinValue, Float.MaxValue)

  implicit val doubleGen: Gen[Double] = Gen.chooseNum(Double.MinValue, Double.MaxValue)
}
