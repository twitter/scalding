package com.twitter.scalding.macros

import cascading.tuple.{ Tuple => CTuple, TupleEntry }

import org.scalatest.{ Matchers, WordSpec }

import com.twitter.scalding._
import com.twitter.scalding.macros._
import com.twitter.scalding.macros.impl._
import com.twitter.scalding.serialization.Externalizer

import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }

// We avoid nesting these just to avoid any complications in the serialization test
case class SampleClassA(x: Int, y: String)
case class SampleClassB(a1: SampleClassA, a2: SampleClassA, y: String)
case class SampleClassC(a: SampleClassA, b: SampleClassB, c: SampleClassA, d: SampleClassB, e: SampleClassB)
case class SampleClassD(a: Option[SampleClassC])
case class SampleClassFail(a: Option[Option[Int]])

class MacrosUnitTests extends WordSpec with Matchers {
  import MacroImplicits._
  def isMg[T](t: T): T = {
    t shouldBe a[MacroGenerated]
    t
  }

  private val dummy = new TupleConverter[Nothing] {
    def apply(te: TupleEntry) = sys.error("dummy")
    override val arity = 1
  }

  def isMacroTupleConverterAvailable[T](implicit proof: TupleConverter[T] = dummy.asInstanceOf[TupleConverter[T]]) =
    proof.isInstanceOf[MacroGenerated]

  def mgConv[T](te: TupleEntry)(implicit conv: TupleConverter[T]): T = isMg(conv)(te)
  def mgSet[T](t: T)(implicit set: TupleSetter[T]): TupleEntry = new TupleEntry(isMg(set)(t))

  def shouldRoundTrip[T: IsCaseClass: TupleSetter: TupleConverter](t: T) {
    t shouldBe mgConv(mgSet(t))
  }

  def shouldRoundTripOther[T: IsCaseClass: TupleSetter: TupleConverter](te: TupleEntry, t: T) {
    val inter = mgConv(te)
    inter shouldBe t
    mgSet(inter) shouldBe te
  }

  def canExternalize(t: AnyRef) { Externalizer(t).javaWorks shouldBe true }

  "MacroGenerated TupleSetter" should {
    def doesJavaWork[T](implicit set: TupleSetter[T]) { canExternalize(isMg(set)) }
    "be serializable for case class A" in { doesJavaWork[SampleClassA] }
    "be serializable for case class B" in { doesJavaWork[SampleClassB] }
    "be serializable for case class C" in { doesJavaWork[SampleClassC] }
    "be serializable for case class D" in { doesJavaWork[SampleClassD] }

  }

  "MacroGenerated TupleConverter" should {
    "Generate the converter SampleClassA" in { Macros.caseClassTupleConverter[SampleClassA] }
    "Generate the converter SampleClassB" in { Macros.caseClassTupleConverter[SampleClassB] }
    "Generate the converter SampleClassC" in { Macros.caseClassTupleConverter[SampleClassC] }
    "Generate the converter SampleClassD" in { Macros.caseClassTupleConverter[SampleClassD] }
    "Not generate a convertor for SampleClassFail" in { isMacroTupleConverterAvailable[SampleClassFail] shouldBe false }

    def doesJavaWork[T](implicit conv: TupleConverter[T]) { canExternalize(isMg(conv)) }
    "be serializable for case class A" in { doesJavaWork[SampleClassA] }
    "be serializable for case class B" in { doesJavaWork[SampleClassB] }
    "be serializable for case class C" in { doesJavaWork[SampleClassC] }
    "be serializable for case class D" in { doesJavaWork[SampleClassD] }
  }

  "MacroGenerated TupleSetter and TupleConverter" should {
    "round trip class -> tupleentry -> class" in {
      shouldRoundTrip(SampleClassA(100, "onehundred"))
      shouldRoundTrip(SampleClassB(SampleClassA(100, "onehundred"), SampleClassA(-1, "zero"), "what"))
      val a = SampleClassA(73, "hrmA1")
      val b = SampleClassB(a, a, "hrmB1")
      val c = SampleClassC(a, b, SampleClassA(123980, "heyA2"), SampleClassB(a, SampleClassA(-1, "zeroA3"), "zooB2"), b)
      shouldRoundTrip(b)
      shouldRoundTrip(c)
      shouldRoundTrip(SampleClassD(Some(c)))
      shouldRoundTrip(SampleClassD(None))
    }

    "Case Class should form expected tuple" in {
      val input = SampleClassC(SampleClassA(1, "asdf"),
        SampleClassB(SampleClassA(2, "bcdf"), SampleClassA(5, "jkfs"), "wetew"),
        SampleClassA(9, "xcmv"),
        SampleClassB(SampleClassA(23, "ck"), SampleClassA(13, "dafk"), "xcv"),
        SampleClassB(SampleClassA(34, "were"), SampleClassA(654, "power"), "adsfmx"))
      val setter = implicitly[TupleSetter[SampleClassC]]
      val tup = setter(input)
      assert(tup.size == 19)
      assert(tup.get(0) === 1)
      assert(tup.get(18) === "adsfmx")
    }

    "round trip tupleentry -> class -> tupleEntry" in {
      val a_tup = CTuple.size(2)
      a_tup.setInteger(0, 100)
      a_tup.setString(1, "onehundred")
      val a_te = new TupleEntry(a_tup)
      val a = SampleClassA(100, "onehundred")
      shouldRoundTripOther(a_te, a)

      val b_tup = CTuple.size(5)
      b_tup.setInteger(0, 100)
      b_tup.setString(1, "onehundred")
      b_tup.setInteger(2, 100)
      b_tup.setString(3, "onehundred")
      b_tup.setString(4, "what")
      val b_te = new TupleEntry(b_tup)
      val b = SampleClassB(a, a, "what")
      shouldRoundTripOther(b_te, b)

      val c_tup = CTuple.size(19)
      c_tup.setInteger(0, 100)
      c_tup.setString(1, "onehundred")

      c_tup.setInteger(2, 100)
      c_tup.setString(3, "onehundred")
      c_tup.setInteger(4, 100)
      c_tup.setString(5, "onehundred")
      c_tup.setString(6, "what")

      c_tup.setInteger(7, 100)
      c_tup.setString(8, "onehundred")

      c_tup.setInteger(9, 100)
      c_tup.setString(10, "onehundred")
      c_tup.setInteger(11, 100)
      c_tup.setString(12, "onehundred")
      c_tup.setString(13, "what")

      c_tup.setInteger(14, 100)
      c_tup.setString(15, "onehundred")
      c_tup.setInteger(16, 100)
      c_tup.setString(17, "onehundred")
      c_tup.setString(18, "what")

      val c_te = new TupleEntry(c_tup)
      val c = SampleClassC(a, b, a, b, b)
      shouldRoundTripOther(c_te, c)
    }

    "Case Class should form expected Fields" in {
      val fields = Macros.toFields[SampleClassB]
      assert(fields.size === 5)
      assert(fields.getTypes === Array[java.lang.reflect.Type](classOf[Int], classOf[String], classOf[Int], classOf[String], classOf[String]))
      val names = List("a1.x", "a1.y", "a2.x", "a2.y", "y")
      names.zipWithIndex.foreach {
        case (name, indx) =>
          assert(fields.get(indx) === name)
      }
    }

    "Case Class should form expected Fields with Options" in {
      val fields = Macros.toFields[SampleClassD]
      assert(fields.size === 19)
      assert(fields.getTypes === Array.fill[java.lang.reflect.Type](19)(classOf[java.lang.Object]))
    }

    "Case Class should form expected Indexed Fields" in {
      val fields = Macros.toIndexedFields[SampleClassB]
      assert(fields.size === 5)
      assert(fields.getTypes === Array[java.lang.reflect.Type](classOf[Int], classOf[String], classOf[Int], classOf[String], classOf[String]))
      val names = (0 until fields.size)
      names.zipWithIndex.foreach {
        case (name, indx) =>
          assert(fields.get(indx) === name)
      }
    }
  }
}
