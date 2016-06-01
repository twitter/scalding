/*
 Copyright 2014 Twitter, Inc.

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
package com.twitter.scalding.macros

import cascading.tuple.{ Tuple => CTuple, TupleEntry }
import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }
import com.twitter.scalding._
import com.twitter.scalding.macros._
import com.twitter.scalding.macros.impl._
import com.twitter.scalding.serialization.Externalizer
import org.scalacheck.Arbitrary
import org.scalacheck.Gen.choose
import org.scalacheck.Prop
import org.scalacheck.Prop.forAll
import org.scalacheck.Properties
import org.scalatest.{ Matchers, WordSpec }
import scala.language.experimental.{ macros => smacros }
import scala.reflect.runtime.universe._

// We avoid nesting these just to avoid any complications in the serialization test
case class SampleClassA(x: Int, y: String)
case class SampleClassB(a1: SampleClassA, a2: SampleClassA, y: String)
case class SampleClassC(a: SampleClassA, b: SampleClassB, c: SampleClassA, d: SampleClassB, e: SampleClassB)
case class SampleClassD(a: Option[SampleClassC])
case class SampleClassE(a: String, b: Boolean, c: Short, d: Int, e: Long, f: Float, g: Double)
case class SampleClassF(a: Option[Int])
case class SampleClassG(a: java.util.Date)

case class SampleClassFail(a: Option[Option[Int]]) // linter:ignore

object MacroProperties extends Properties("TypeDescriptor.roundTrip") {
  def roundTrip[T: Arbitrary: TypeDescriptor]: Prop = forAll { t: T =>
    val setter = implicitly[TypeDescriptor[T]].setter
    val converter = implicitly[TypeDescriptor[T]].converter
    val fields = implicitly[TypeDescriptor[T]].fields
    converter(new TupleEntry(fields, setter(t))) == t
  }

  def propertyFor[T: TypeTag: Arbitrary: TypeDescriptor]: Unit = {
    property(typeTag[T].tpe.toString) = roundTrip[T]
  }

  propertyFor[Int]
  propertyFor[Option[Int]]
  propertyFor[Option[(Int, String, Option[Long])]]
  propertyFor[Option[(Option[Boolean], Int, String, Option[Long])]]
  propertyFor[(Int, Double, String, Option[(String, Int, Option[Long])])]
}

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

  private val dummy2 = new TypeDescriptor[Nothing] {
    def setter = sys.error("dummy")
    def converter = sys.error("dummy")
    def fields = sys.error("dummy")
  }

  def isMacroTupleConverterAvailable[T](implicit proof: TupleConverter[T] = dummy.asInstanceOf[TupleConverter[T]]) =
    proof.isInstanceOf[MacroGenerated]

  def isMacroTypeDescriptorAvailable[T](implicit proof: TypeDescriptor[T] = dummy2.asInstanceOf[TypeDescriptor[T]]) =
    proof.isInstanceOf[MacroGenerated]

  def mgConv[T](te: TupleEntry)(implicit conv: TupleConverter[T]): T = isMg(conv)(te)
  def mgSet[T](t: T)(implicit set: TupleSetter[T]): TupleEntry = new TupleEntry(isMg(set)(t))

  def shouldRoundTrip[T: IsCaseClass: TupleSetter: TupleConverter](t: T): Unit = {
    t shouldBe mgConv(mgSet(t))
  }

  def shouldRoundTripOther[T: IsCaseClass: TupleSetter: TupleConverter](te: TupleEntry, t: T): Unit = {
    val inter = mgConv(te)
    inter shouldBe t
    mgSet(inter) shouldBe te
  }

  def canExternalize(t: AnyRef): Unit = {
    Externalizer(t).javaWorks shouldBe true
  }

  "MacroGenerated TupleConverter" should {
    "Not compile for Option[Option[Int]]" in {
      //TODO figure out a way to test this does not compile. See:
      //https://github.com/milessabin/shapeless/blob/master/core/src/main/scala/shapeless/test/typechecking.scala
      //uncommenting fails to compile, but we want to be more sure
      //Macros.caseClassTupleConverter[Option[Option[Int]]]
      //Macros.caseClassTupleConverter[Option[String]]
    }
  }

  "MacroGenerated TupleSetter" should {

    "Generate the setter SampleClassA" in { Macros.caseClassTupleSetter[SampleClassA] }
    "Generate the setter SampleClassB" in { Macros.caseClassTupleSetter[SampleClassB] }
    "Generate the setter SampleClassC" in { Macros.caseClassTupleSetter[SampleClassC] }
    "Generate the setter SampleClassD" in { Macros.caseClassTupleSetter[SampleClassD] }
    "Generate the setter SampleClassE" in { Macros.caseClassTupleSetter[SampleClassE] }
    "Generate the setter SampleClassF" in { Macros.caseClassTupleSetter[SampleClassF] }
    "Generate the setter SampleClassG" in { Macros.caseClassTupleSetterWithUnknown[SampleClassG] }

    def doesJavaWork[T](implicit set: TupleSetter[T]): Unit =
      canExternalize(isMg(set))
    "be serializable for case class A" in { doesJavaWork[SampleClassA] }
    "be serializable for case class B" in { doesJavaWork[SampleClassB] }
    "be serializable for case class C" in { doesJavaWork[SampleClassC] }
    "be serializable for case class D" in { doesJavaWork[SampleClassD] }
    "be serializable for case class E" in { doesJavaWork[SampleClassE] }
    "be serializable for case class F" in { doesJavaWork[SampleClassF] }
  }

  "MacroGenerated TupleConverter" should {
    "Generate the converter SampleClassA" in { Macros.caseClassTupleConverter[SampleClassA] }
    "Generate the converter SampleClassB" in { Macros.caseClassTupleConverter[SampleClassB] }
    "Generate the converter SampleClassC" in { Macros.caseClassTupleConverter[SampleClassC] }
    "Generate the converter SampleClassD" in { Macros.caseClassTupleConverter[SampleClassD] }
    "Generate the converter SampleClassE" in { Macros.caseClassTupleConverter[SampleClassE] }
    "Generate the converter SampleClassF" in { Macros.caseClassTupleConverter[SampleClassF] }
    "Generate the converter SampleClassG" in { Macros.caseClassTupleConverterWithUnknown[SampleClassG] }
    "Generate the converter Option[(Int, String)]" in { Macros.caseClassTupleConverter[Option[(Int, String)]] }
    "Generate the converter Option[(Int, Option[(Long, String)])]" in {
      Macros.caseClassTupleConverter[Option[(Int, Option[(Long, String)])]]
    }

    "Not generate a convertor for SampleClassFail" in { isMacroTupleConverterAvailable[SampleClassFail] shouldBe false }

    def doesJavaWork[T](implicit conv: TupleConverter[T]): Unit =
      canExternalize(isMg(conv))
    "be serializable for case class A" in { doesJavaWork[SampleClassA] }
    "be serializable for case class B" in { doesJavaWork[SampleClassB] }
    "be serializable for case class C" in { doesJavaWork[SampleClassC] }
    "be serializable for case class D" in { doesJavaWork[SampleClassD] }
    "be serializable for case class E" in { doesJavaWork[SampleClassE] }
    "be serializable for case class F" in { doesJavaWork[SampleClassF] }
  }

  "MacroGenerated TypeDescriptor" should {
    "Generate the converter SampleClassA" in { Macros.caseClassTypeDescriptor[SampleClassA] }
    "Generate the converter SampleClassB" in { Macros.caseClassTypeDescriptor[SampleClassB] }
    "Generate the converter SampleClassC" in { Macros.caseClassTypeDescriptor[SampleClassC] }
    "Generate the converter SampleClassD" in { Macros.caseClassTypeDescriptor[SampleClassD] }
    "Generate the converter SampleClassE" in { Macros.caseClassTypeDescriptor[SampleClassE] }
    "Generate the converter SampleClassF" in { Macros.caseClassTypeDescriptor[SampleClassF] }
    "Generate the converter SampleClassG" in { Macros.caseClassTypeDescriptorWithUnknown[SampleClassG] }

    "Not generate a convertor for SampleClassFail" in { isMacroTypeDescriptorAvailable[SampleClassFail] shouldBe false }

    def doesJavaWork[T](implicit conv: TypeDescriptor[T]): Unit =
      canExternalize(isMg(conv))
    "be serializable for case class A" in { doesJavaWork[SampleClassA] }
    "be serializable for case class B" in { doesJavaWork[SampleClassB] }
    "be serializable for case class C" in { doesJavaWork[SampleClassC] }
    "be serializable for case class D" in { doesJavaWork[SampleClassD] }
    "be serializable for case class E" in { doesJavaWork[SampleClassE] }
    "be serializable for case class F" in { doesJavaWork[SampleClassF] }
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

      implicit val tupSetterG = Macros.caseClassTupleSetterWithUnknown[SampleClassG]
      implicit val tupConverterG = Macros.caseClassTupleConverterWithUnknown[SampleClassG]
      shouldRoundTrip(SampleClassG(new java.util.Date(123412L)))
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
      assert(tup.getInteger(0) === 1)
      assert(tup.getString(18) === "adsfmx")
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

    "Case Class should form expected Fields with Unknown types" in {
      val fields = Macros.toFieldsWithUnknown[SampleClassG]
      assert(fields.size === 1)
      assert(fields.getTypes === Array[java.lang.reflect.Type](classOf[java.util.Date]))
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
