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

package com.twitter.scalding.macros

import org.scalatest.{ FunSuite, ShouldMatchers }
import org.scalatest.prop.PropertyChecks
import scala.language.experimental.macros
import com.twitter.scalding.typed.OrderedBufferable
import java.nio.ByteBuffer
import org.scalacheck.Arbitrary.{ arbitrary => arb }
import com.twitter.bijection.Bufferable
import org.scalacheck.{ Arbitrary, Gen, Prop }

trait LowerPriorityImplicit {
  implicit def primitiveOrderedBufferSupplier[T] = macro com.twitter.scalding.macros.impl.OrderedBufferableProviderImpl[T]
}

object ByteBufferArb {
  implicit def arbitraryTestTypes: Arbitrary[ByteBuffer] = Arbitrary {
    for {
      aBinary <- Gen.alphaStr.map(s => ByteBuffer.wrap(s.getBytes("UTF-8")))
    } yield aBinary
  }
}
object TestCC {
  import ByteBufferArb._
  implicit def arbitraryTestCC: Arbitrary[TestCC] = Arbitrary {
    for {
      aInt <- arb[Int]
      aLong <- arb[Long]
      aDouble <- arb[Double]
      anOption <- arb[Option[Int]]
      anStrOption <- arb[Option[String]]
      anOptionOfAListOfStrings <- arb[Option[List[String]]]
      aBB <- arb[ByteBuffer]
    } yield TestCC(aInt, aLong, anOption, aDouble, anStrOption, anOptionOfAListOfStrings, aBB)
  }
}
case class TestCC(a: Int, b: Long, c: Option[Int], d: Double, e: Option[String], f: Option[List[String]], aBB: ByteBuffer)

object MyData {
  implicit def arbitraryTestCC: Arbitrary[MyData] = Arbitrary {
    for {
      aInt <- arb[Int]
      anOption <- arb[Option[Long]]
    } yield new MyData(aInt, anOption)
  }
}

class MyData(override val _1: Int, override val _2: Option[Long]) extends Product2[Int, Option[Long]] {
  override def canEqual(that: Any): Boolean = that match {
    case o: MyData => this._1 == o._1 && this._2 == o._2
    case _ => false
  }
}

object Container {
  implicit def arbitraryInnerCaseClass: Arbitrary[InnerCaseClass] = Arbitrary {
    for {
      anOption <- arb[Set[Double]]
    } yield InnerCaseClass(anOption)
  }

  type SetAlias = Set[Double]
  case class InnerCaseClass(e: SetAlias)
}
class MacroOrderingProperties extends FunSuite with PropertyChecks with ShouldMatchers with LowerPriorityImplicit {

  type SetAlias = Set[Double]

  import ByteBufferArb._
  import Container.arbitraryInnerCaseClass

  def serialize[T](t: T)(implicit orderedBuffer: OrderedBufferable[T]): ByteBuffer =
    serializeSeq(List(t))

  def serializeSeq[T](t: Seq[T])(implicit orderedBuffer: OrderedBufferable[T]): ByteBuffer = {

    def innerSerialize(t: Seq[T], buffSize: Int)(implicit orderedBuffer: OrderedBufferable[T]): ByteBuffer = {
      val buf = ByteBuffer.allocate(buffSize)
      try {
        t.foldLeft(buf) {
          case (innerBuf, innerT) =>
            orderedBuffer.put(innerBuf, innerT)
        }
      } catch {
        case _: java.nio.BufferOverflowException => innerSerialize(t, buffSize * 2)
      }
    }

    val bb = innerSerialize(t, 128)
    bb.position(0)
    bb

  }

  def rt[T](t: T)(implicit orderedBuffer: OrderedBufferable[T]) = {
    val buf = serialize[T](t)
    orderedBuffer.get(buf).map(_._2).get
  }

  def rawCompare[T](a: T, b: T)(implicit obuf: OrderedBufferable[T]): Int = {
    obuf.compareBinary(serialize(a), serialize(b)).unsafeToInt
  }

  def oBufCompare[T](a: T, b: T)(implicit obuf: OrderedBufferable[T]): Int = {
    obuf.compare(a, b)
  }

  def checkManyExplicit[T](i: List[T])(implicit obuf: OrderedBufferable[T]) = {
    val serializedA = serializeSeq(i)
    val serializedB = serializedA.duplicate
    (0 until i.size).foreach { _ =>
      assert(obuf.compareBinary(serializedA, serializedB).unsafeToInt === 0)
    }
  }

  def checkMany[T: Arbitrary](implicit ord: Ordering[T], obuf: OrderedBufferable[T]) = forAll { i: List[T] =>
    checkManyExplicit(i)
  }

  def clamp(i: Int): Int =
    i match {
      case x if x < 0 => -1
      case x if x > 0 => 1
      case x => 0
    }

  def check[T: Arbitrary](implicit ord: Ordering[T], obuf: OrderedBufferable[T]) = forAll { (a: T, b: T) =>
    rt(a) // before we do anything ensure these don't throw
    rt(b) // before we do anything ensure these don't throw
    assert(oBufCompare(rt(a), a) === 0, s"A should be equal to itself after an RT -- ${rt(a)}")
    assert(oBufCompare(rt(b), b) === 0, "B should be equal to itself after an RT-- ${rt(b)}")
    assert(oBufCompare(a, b) + oBufCompare(b, a) === 0, "In memory comparasons make sense")
    assert(rawCompare(a, b) + rawCompare(b, a) === 0, "When adding the raw compares in inverse order they should sum to 0")
    assert(oBufCompare(rt(a), rt(b)) === oBufCompare(a, b), "Comparing a and b with ordered bufferables compare after a serialization RT")
    assert(rawCompare(a, b) === clamp(ord.compare(a, b)), "Raw and in memory compares match")
    assert(oBufCompare(a, b) === clamp(ord.compare(a, b)), "Our ordered bufferable in memory matches a normal one")
  }

  def checkWithInputs[T](a: T, b: T)(implicit obuf: OrderedBufferable[T]) {
    rt(a) // before we do anything ensure these don't throw
    rt(b) // before we do anything ensure these don't throw
    assert(oBufCompare(rt(a), a) === 0, s"A should be equal to itself after an RT -- ${rt(a)}")
    assert(oBufCompare(rt(b), b) === 0, s"B should be equal to itself after an RT-- ${rt(b)}")
    assert(oBufCompare(a, b) + oBufCompare(b, a) === 0, "In memory comparasons make sense")
    assert(rawCompare(a, b) + rawCompare(b, a) === 0, "When adding the raw compares in inverse order they should sum to 0")
    assert(oBufCompare(rt(a), rt(b)) === oBufCompare(a, b), "Comparing a and b with ordered bufferables compare after a serialization RT")
  }

  def checkAreSame[T](a: T, b: T)(implicit obuf: OrderedBufferable[T]) {
    rt(a) // before we do anything ensure these don't throw
    rt(b) // before we do anything ensure these don't throw
    assert(oBufCompare(rt(a), a) === 0, s"A should be equal to itself after an RT -- ${rt(a)}")
    assert(oBufCompare(rt(b), b) === 0, "B should be equal to itself after an RT-- ${rt(b)}")
    assert(oBufCompare(a, b) === 0, "In memory comparasons make sense")
    assert(oBufCompare(b, a) === 0, "In memory comparasons make sense")
    assert(rawCompare(a, b) === 0, "When adding the raw compares in inverse order they should sum to 0")
    assert(rawCompare(b, a) === 0, "When adding the raw compares in inverse order they should sum to 0")
    assert(oBufCompare(rt(a), rt(b)) === oBufCompare(a, b), "Comparing a and b with ordered bufferables compare after a serialization RT")
  }

  def checkWithoutOrd[T: Arbitrary](implicit obuf: OrderedBufferable[T]) = forAll(minSuccessful(500)) { (a: T, b: T) =>
    checkWithInputs(a, b)
  }

  test("Test out Int") {
    val localOrdering = Ordering.ordered[Int](identity)
    check[Int](implicitly, localOrdering, implicitly)
    checkMany[Int]
  }

  test("Test out Long") {
    implicit val localOrdering = Ordering.ordered[Long](identity)
    check[Long]
  }

  test("Test out Short") {
    implicit val localOrdering = Ordering.ordered[Short](identity)
    check[Short]
  }

  test("Test out Float") {
    implicit val localOrdering = Ordering.ordered[Float](identity)
    check[Float]
  }

  test("Test out Boolean") {
    implicit val localOrdering = Ordering.ordered[Boolean](identity)
    check[Boolean]
  }

  test("Test out ByteBuffer") {
    primitiveOrderedBufferSupplier[ByteBuffer]
    checkWithoutOrd[ByteBuffer]
  }

  test("Test out List[Float]") {
    primitiveOrderedBufferSupplier[List[Float]]
    check[List[Float]]
  }

  test("Test out List[Int]") {
    primitiveOrderedBufferSupplier[List[Int]]
    check[List[Int]]
  }

  test("Test out SetAlias") {
    primitiveOrderedBufferSupplier[SetAlias]
    check[SetAlias]
  }

  test("Container.InnerCaseClass") {
    primitiveOrderedBufferSupplier[Container.InnerCaseClass]
    check[Container.InnerCaseClass]
  }

  test("Test out Seq[Int]") {
    primitiveOrderedBufferSupplier[Seq[Int]]
    check[Seq[Int]]
  }

  test("Test out Array[Byte]") {
    primitiveOrderedBufferSupplier[Array[Byte]]
    check[Array[Byte]]
  }

  test("Test out Vector[Int]") {
    primitiveOrderedBufferSupplier[Vector[Int]]
    check[Vector[Int]]
  }

  test("Test out Set[Int]") {
    primitiveOrderedBufferSupplier[Set[Int]]
    checkWithoutOrd[Set[Int]]
  }

  test("Test out Set[Double]") {
    primitiveOrderedBufferSupplier[Set[Double]]
    checkWithoutOrd[Set[Double]]
  }

  test("Test out Map[Set[Int], Long]") {
    primitiveOrderedBufferSupplier[Map[Set[Int], Long]]
    checkWithoutOrd[Map[Set[Int], Long]]
    val c = List(Map(Set[Int]() -> 9223372036854775807L), Map(Set[Int](-2043106012) -> -1L))
    checkManyExplicit(c)
    checkMany[Map[Set[Int], Long]]
  }

  test("Test out Map[Long, Long]") {
    primitiveOrderedBufferSupplier[Map[Long, Long]]
    checkWithoutOrd[Map[Long, Long]]
  }

  test("Test out comparing Maps(3->2, 2->3) and Maps(2->3, 3->2) ") {
    val a = Map(3 -> 2, 2 -> 3)
    val b = Map(2 -> 3, 3 -> 2)
    checkWithInputs(a, b)
    checkAreSame(a, b)
  }

  test("Test out comparing Set(\"asdf\", \"jkl\") and  Set(\"jkl\", \"asdf\")") {
    val a = Set("asdf", "jkl")
    val b = Set("jkl", "asdf")
    checkWithInputs(a, b)
    checkAreSame(a, b)
  }

  test("Test out Double") {
    implicit val localOrdering = Ordering.ordered[Double](identity)
    check[Double]
  }

  test("Test out Char") {
    implicit val localOrdering = Ordering.ordered[Char](identity)
    check[Char]
  }

  test("Test out Byte") {
    implicit val localOrdering = Ordering.ordered[Byte](identity)
    check[Byte]
  }

  test("Test out String") {
    primitiveOrderedBufferSupplier[String]
    val localOrdering = Ordering.String
    check[String](implicitly, localOrdering, implicitly)
    checkMany[String]
  }

  test("Test known hard String Case") {
    val a = "6"
    val b = "곆"
    val ord = Ordering.String
    assert(rawCompare(a, b) === clamp(ord.compare(a, b)), "Raw and in memory compares match")

    val c = List("榴㉕⊟풠湜ᙬ覹ꜻ裧뚐⠂覝쫨塢䇺楠谭픚ᐌ轮뺷Ⱟ洦擄黏著탅ﮓꆋ숷梸傠ァ蹵窥轲闇涡飽ꌳ䝞慙擃",
      "堒凳媨쉏떽㶥⾽샣井ㆠᇗ裉깴辫࠷᤭塈䎙寫㸉ᶴ䰄똇䡷䥞㷗䷱赫懓䷏剆祲ᝯ졑쐯헢鷴ӕ秔㽰ퟡ㏉鶖奚㙰银䮌ᕗ膾买씋썴행䣈丶偝쾕鐗쇊ኋ넥︇瞤䋗噯邧⹆♣ἷ铆玼⪷沕辤ᠥ⥰箼䔄◗",
      "騰쓢堷뛭ᣣﰩ嚲ﲯ㤑ᐜ檊೦⠩奯ᓩ윇롇러ᕰెꡩ璞﫼᭵礀閮䈦椄뾪ɔ믻䖔᪆嬽ﾌ鶬曭꣍ᆏ灖㐸뗋ㆃ녵ퟸ겵晬礙㇩䫓ᘞ昑싨",
      "좃ఱ䨻綛糔唄࿁劸酊᫵橻쩳괊筆ݓ淤숪輡斋靑耜঄骐冠㝑⧠떅漫곡祈䵾ᳺ줵됵↲搸虂㔢Ꝅ芆٠풐쮋炞哙⨗쾄톄멛癔짍避쇜畾㣕剼⫁়╢ꅢ澛氌ᄚ㍠ꃫᛔ匙㜗詇閦單錖⒅瘧崥",
      "獌癚畇")
    checkManyExplicit(c)

    val c2 = List("聸", "")
    checkManyExplicit(c2)
  }

  test("Test out Option[Int]") {
    primitiveOrderedBufferSupplier[Option[Int]]
    val localOrdering = Ordering.Option(Ordering.Int)
    check[Option[Int]](implicitly, localOrdering, implicitly)
    checkMany[Option[Int]]
  }

  test("Test out Option[String]") {
    primitiveOrderedBufferSupplier[Option[String]]
    val localOrdering = Ordering.Option(Ordering.String)
    check[Option[String]](implicitly, localOrdering, implicitly)
    checkMany[Option[String]]
  }

  test("Test out Option[Option[Int]]") {
    implicit val localOrdering = Ordering.Option(Ordering.Option(Ordering.Int))
    check[Option[Option[Int]]]
  }

  test("test product like TestCC") {
    checkMany[(Int, Long, Option[Int], Double, Option[String])]
  }

  test("Test out TestCC") {
    import TestCC._
    primitiveOrderedBufferSupplier[TestCC]
    checkWithoutOrd[TestCC]
    checkMany[TestCC]
  }

  test("Test out (Int, Int)") {
    primitiveOrderedBufferSupplier[(Int, Int)]
    check[(Int, Int)](implicitly[Arbitrary[(Int, Int)]], Ordering.Tuple2, implicitly[OrderedBufferable[(Int, Int)]])
  }

  test("Test out (String, Option[Int], String)") {
    primitiveOrderedBufferSupplier[(String, Option[Int], String)]
    checkWithoutOrd[(String, Option[Int], String)]
  }

  test("Test out MyData") {
    import MyData._
    primitiveOrderedBufferSupplier[MyData]
    checkWithoutOrd[MyData]
  }

}