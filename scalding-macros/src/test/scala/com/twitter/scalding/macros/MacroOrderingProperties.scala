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
import org.scalatest.prop.Checkers
import org.scalatest.prop.PropertyChecks
import scala.language.experimental.macros
import com.twitter.scalding.serialization.{ OrderedSerialization, Law, Law1, Law2, Law3, Serialization }
import java.nio.ByteBuffer
import org.scalacheck.Arbitrary.{ arbitrary => arb }
import java.io.{ ByteArrayOutputStream, InputStream }

import com.twitter.bijection.Bufferable
import org.scalacheck.{ Arbitrary, Gen, Prop }
import com.twitter.scalding.serialization.JavaStreamEnrichments

import scala.collection.immutable.Queue

trait LowerPriorityImplicit {
  implicit def primitiveOrderedBufferSupplier[T] = macro com.twitter.scalding.macros.impl.OrderedSerializationProviderImpl[T]
}

object LawTester {
  def apply[T: Arbitrary](laws: Iterable[Law[T]]): Prop =
    apply(implicitly[Arbitrary[T]].arbitrary, laws)

  def apply[T](g: Gen[T], laws: Iterable[Law[T]]): Prop =
    laws.foldLeft(true: Prop) {
      case (soFar, Law1(name, fn)) => soFar && Prop.forAll(g)(fn).label(name)
      case (soFar, Law2(name, fn)) => soFar && Prop.forAll(g, g)(fn).label(name)
      case (soFar, Law3(name, fn)) => soFar && Prop.forAll(g, g, g)(fn).label(name)
    }
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

object MacroOpaqueContainer {
  import java.io._
  implicit val myContainerOrderedSerializer = new OrderedSerialization[MacroOpaqueContainer] {
    val intOrderedSerialization = _root_.com.twitter.scalding.macros.Macros.orderedBufferSupplier[Int]

    override def hash(s: MacroOpaqueContainer) = intOrderedSerialization.hash(s.myField) ^ Int.MaxValue
    override def compare(a: MacroOpaqueContainer, b: MacroOpaqueContainer) = intOrderedSerialization.compare(a.myField, b.myField)

    override def read(in: InputStream) = intOrderedSerialization.read(in).map(MacroOpaqueContainer(_))

    override def write(b: OutputStream, s: MacroOpaqueContainer) = intOrderedSerialization.write(b, s.myField)

    override def compareBinary(lhs: InputStream, rhs: InputStream) = intOrderedSerialization.compareBinary(lhs, rhs)
    override def staticSize = Some(4)
  }

  implicit def arbitraryMacroOpaqueContainer: Arbitrary[MacroOpaqueContainer] = Arbitrary {
    for {
      aInt <- arb[Int]
    } yield MacroOpaqueContainer(aInt)
  }

  def apply(d: Int): MacroOpaqueContainer = new MacroOpaqueContainer(d)
}

class MacroOpaqueContainer(val myField: Int) {
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

  import OrderedSerialization.{ compare => oBufCompare }

  def gen[T: Arbitrary]: Gen[T] = implicitly[Arbitrary[T]].arbitrary

  def collectionArb[C[_], T: Arbitrary](implicit cbf: collection.generic.CanBuildFrom[Nothing, T, C[T]]): Arbitrary[C[T]] = Arbitrary {
    gen[List[T]].map { l =>
      val builder = cbf()
      l.foreach { builder += _ }
      builder.result
    }
  }

  def serialize[T](t: T)(implicit orderedBuffer: OrderedSerialization[T]): InputStream =
    serializeSeq(List(t))

  def serializeSeq[T](t: Seq[T])(implicit orderedBuffer: OrderedSerialization[T]): InputStream = {
    import JavaStreamEnrichments._

    val baos = new ByteArrayOutputStream
    t.foreach({ e =>
      orderedBuffer.write(baos, e)
    })
    baos.toInputStream
  }

  def rt[T](t: T)(implicit orderedBuffer: OrderedSerialization[T]) = {
    val buf = serialize[T](t)
    orderedBuffer.read(buf).get
  }

  def rawCompare[T](a: T, b: T)(implicit obuf: OrderedSerialization[T]): Int =
    obuf.compareBinary(serialize(a), serialize(b)).unsafeToInt

  def checkManyExplicit[T](i: List[(T, T)])(implicit obuf: OrderedSerialization[T]) = {
    val serializedA = serializeSeq(i.map(_._1))
    val serializedB = serializeSeq(i.map(_._2))
    i.foreach {
      case (a, b) =>
        val compareBinary = obuf.compareBinary(serializedA, serializedB).unsafeToInt
        val compareMem = obuf.compare(a, b)
        if (compareBinary < 0) {
          assert(compareMem < 0, s"Compare binary: $compareBinary, and compareMem : $compareMem must have the same sign")
        } else if (compareBinary > 0) {
          assert(compareMem > 0, s"Compare binary: $compareBinary, and compareMem : $compareMem must have the same sign")
        }
    }
  }

  def checkMany[T: Arbitrary](implicit obuf: OrderedSerialization[T]) = forAll { i: List[(T, T)] =>
    checkManyExplicit(i)
  }

  def clamp(i: Int): Int =
    i match {
      case x if x < 0 => -1
      case x if x > 0 => 1
      case x => 0
    }

  def checkWithInputs[T](a: T, b: T)(implicit obuf: OrderedSerialization[T]) {
    val rta = rt(a) // before we do anything ensure these don't throw
    val rtb = rt(b) // before we do anything ensure these don't throw
    val asize = Serialization.toBytes(a).length
    if (obuf.dynamicSize(a).isDefined) {
      assert(obuf.dynamicSize(a).get == asize, "dynamic size matches the correct value")
    }
    if (obuf.staticSize.isDefined) {
      assert(obuf.dynamicSize(a).get == asize, "dynamic size matches the correct value")
      assert(obuf.staticSize.get == asize, "dynamic size matches the correct value")
    }
    assert(oBufCompare(rta, a) === 0, s"A should be equal to itself after an RT -- ${rt(a)}")
    assert(oBufCompare(rtb, b) === 0, s"B should be equal to itself after an RT-- ${rt(b)}")
    assert(oBufCompare(a, b) + oBufCompare(b, a) === 0, "In memory comparasons make sense")
    assert(rawCompare(a, b) + rawCompare(b, a) === 0, "When adding the raw compares in inverse order they should sum to 0")
    assert(oBufCompare(rta, rtb) === oBufCompare(a, b), "Comparing a and b with ordered bufferables compare after a serialization RT")
  }

  def checkAreSame[T](a: T, b: T)(implicit obuf: OrderedSerialization[T]) {
    val rta = rt(a) // before we do anything ensure these don't throw
    val rtb = rt(b) // before we do anything ensure these don't throw
    assert(oBufCompare(rta, a) === 0, s"A should be equal to itself after an RT -- ${rt(a)}")
    assert(oBufCompare(rtb, b) === 0, "B should be equal to itself after an RT-- ${rt(b)}")
    assert(oBufCompare(a, b) === 0, "In memory comparasons make sense")
    assert(oBufCompare(b, a) === 0, "In memory comparasons make sense")
    assert(rawCompare(a, b) === 0, "When adding the raw compares in inverse order they should sum to 0")
    assert(rawCompare(b, a) === 0, "When adding the raw compares in inverse order they should sum to 0")
    assert(oBufCompare(rta, rtb) === 0, "Comparing a and b with ordered bufferables compare after a serialization RT")
  }

  def check[T: Arbitrary](implicit obuf: OrderedSerialization[T]) = {
    Checkers.check(LawTester(OrderedSerialization.allLaws))
    forAll(minSuccessful(500)) { (a: T, b: T) => checkWithInputs(a, b) }
  }

  test("Test out Unit") {
    primitiveOrderedBufferSupplier[Unit]

    check[Unit]
    checkMany[Unit]
  }

  test("Test out Int") {
    primitiveOrderedBufferSupplier[Int]

    check[Int]
    checkMany[Int]
  }

  test("Test out Long") {
    check[Long]
  }

  test("Test out Short") {

    check[Short]
  }

  test("Test out Float") {

    check[Float]
  }

  test("Test out Boolean") {
    primitiveOrderedBufferSupplier[Boolean]
    check[Boolean]
  }

  test("Test out ByteBuffer") {
    primitiveOrderedBufferSupplier[ByteBuffer]
    check[ByteBuffer]
  }

  test("Test out List[Float]") {
    primitiveOrderedBufferSupplier[List[Float]]
    check[List[Float]]
  }
  test("Test out Queue[Int]") {
    implicit val isa = collectionArb[Queue, Int]
    primitiveOrderedBufferSupplier[Queue[Int]]
    check[Queue[Int]]
  }
  test("Test out IndexedSeq[Int]") {
    implicit val isa = collectionArb[IndexedSeq, Int]
    primitiveOrderedBufferSupplier[IndexedSeq[Int]]
    check[IndexedSeq[Int]]
  }
  test("Test out HashSet[Int]") {
    import scala.collection.immutable.HashSet
    implicit val isa = collectionArb[HashSet, Int]
    primitiveOrderedBufferSupplier[HashSet[Int]]
    check[HashSet[Int]]
  }
  test("Test out ListSet[Int]") {
    import scala.collection.immutable.ListSet
    implicit val isa = collectionArb[ListSet, Int]
    primitiveOrderedBufferSupplier[ListSet[Int]]
    check[ListSet[Int]]
  }

  test("Test out List[String]") {
    primitiveOrderedBufferSupplier[List[String]]
    check[List[String]]
  }

  test("Test out List[List[String]]") {
    val oBuf = primitiveOrderedBufferSupplier[List[List[String]]]
    assert(oBuf.dynamicSize(List(List("sdf"))) === None)
    check[List[List[String]]]
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
  test("Test out scala.collection.Seq[Int]") {
    primitiveOrderedBufferSupplier[scala.collection.Seq[Int]]
    check[scala.collection.Seq[Int]]
  }

  test("Test out Array[Byte]") {
    primitiveOrderedBufferSupplier[Array[Byte]]
    check[Array[Byte]]
  }

  test("Test out Vector[Int]") {
    primitiveOrderedBufferSupplier[Vector[Int]]
    check[Vector[Int]]
  }

  test("Test out Iterable[Int]") {
    primitiveOrderedBufferSupplier[Iterable[Int]]
    check[Iterable[Int]]
  }

  test("Test out Set[Int]") {
    primitiveOrderedBufferSupplier[Set[Int]]
    check[Set[Int]]
  }

  test("Test out Set[Double]") {
    primitiveOrderedBufferSupplier[Set[Double]]
    check[Set[Double]]
  }

  test("Test out Map[Long, Set[Int]]") {
    primitiveOrderedBufferSupplier[Map[Long, Set[Int]]]
    check[Map[Long, Set[Int]]]
    val c = List(Map(9223372036854775807L -> Set[Int]()), Map(-1L -> Set[Int](-2043106012)))
    checkManyExplicit(c.map { i => (i, i) })
    checkMany[Map[Long, Set[Int]]]
  }

  test("Test out Map[Long, Long]") {
    primitiveOrderedBufferSupplier[Map[Long, Long]]
    check[Map[Long, Long]]
  }
  test("Test out HashMap[Long, Long]") {
    import scala.collection.immutable.HashMap
    implicit val isa = Arbitrary(implicitly[Arbitrary[List[(Long, Long)]]].arbitrary.map(HashMap(_: _*)))
    primitiveOrderedBufferSupplier[HashMap[Long, Long]]
    check[HashMap[Long, Long]]
  }
  test("Test out ListMap[Long, Long]") {
    import scala.collection.immutable.ListMap
    implicit val isa = Arbitrary(implicitly[Arbitrary[List[(Long, Long)]]].arbitrary.map(ListMap(_: _*)))
    primitiveOrderedBufferSupplier[ListMap[Long, Long]]
    check[ListMap[Long, Long]]
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

    check[Double]
  }

  test("Test out Byte") {

    check[Byte]
  }

  test("Test out String") {
    primitiveOrderedBufferSupplier[String]

    check[String]
    checkMany[String]
  }

  test("Test known hard String Case") {
    val a = "6"
    val b = "곆"
    val ord = Ordering.String
    assert(rawCompare(a, b) === clamp(ord.compare(a, b)), "Raw and in memory compares match.")

    val c = List("榴㉕⊟풠湜ᙬ覹ꜻ裧뚐⠂覝쫨塢䇺楠谭픚ᐌ轮뺷Ⱟ洦擄黏著탅ﮓꆋ숷梸傠ァ蹵窥轲闇涡飽ꌳ䝞慙擃",
      "堒凳媨쉏떽㶥⾽샣井ㆠᇗ裉깴辫࠷᤭塈䎙寫㸉ᶴ䰄똇䡷䥞㷗䷱赫懓䷏剆祲ᝯ졑쐯헢鷴ӕ秔㽰ퟡ㏉鶖奚㙰银䮌ᕗ膾买씋썴행䣈丶偝쾕鐗쇊ኋ넥︇瞤䋗噯邧⹆♣ἷ铆玼⪷沕辤ᠥ⥰箼䔄◗",
      "騰쓢堷뛭ᣣﰩ嚲ﲯ㤑ᐜ檊೦⠩奯ᓩ윇롇러ᕰెꡩ璞﫼᭵礀閮䈦椄뾪ɔ믻䖔᪆嬽ﾌ鶬曭꣍ᆏ灖㐸뗋ㆃ녵ퟸ겵晬礙㇩䫓ᘞ昑싨",
      "좃ఱ䨻綛糔唄࿁劸酊᫵橻쩳괊筆ݓ淤숪輡斋靑耜঄骐冠㝑⧠떅漫곡祈䵾ᳺ줵됵↲搸虂㔢Ꝅ芆٠풐쮋炞哙⨗쾄톄멛癔짍避쇜畾㣕剼⫁়╢ꅢ澛氌ᄚ㍠ꃫᛔ匙㜗詇閦單錖⒅瘧崥",
      "獌癚畇")
    checkManyExplicit(c.map { i => (i, i) })

    val c2 = List("聸", "")
    checkManyExplicit(c2.map { i => (i, i) })
  }

  test("Test out Option[Int]") {
    val oser = primitiveOrderedBufferSupplier[Option[Int]]

    assert(oser.staticSize === None, "can't get the size statically")
    check[Option[Int]]
    checkMany[Option[Int]]
  }

  test("Test out Option[String]") {
    primitiveOrderedBufferSupplier[Option[String]]

    check[Option[String]]
    checkMany[Option[String]]
  }

  test("Test Either[Int, Option[Int]]") {
    val oser = primitiveOrderedBufferSupplier[Either[Int, Option[Int]]]
    assert(oser.staticSize === None, "can't get the size statically")
    check[Either[Int, Option[Int]]]
  }
  test("Test Either[Int, String]") {
    val oser = primitiveOrderedBufferSupplier[Either[Int, String]]
    assert(oser.staticSize === None, "can't get the size statically")
    assert(Some(Serialization.toBytes[Either[Int, String]](Left(1)).length) === oser.dynamicSize(Left(1)),
      "serialization size matches dynamic size")
    check[Either[Int, String]]
  }
  test("Test Either[Int, Int]") {
    val oser = primitiveOrderedBufferSupplier[Either[Int, Int]]
    assert(oser.staticSize === Some(5), "can get the size statically")
    check[Either[Int, Int]]
  }
  test("Test Either[String, Int]") {
    primitiveOrderedBufferSupplier[Either[String, Int]]
    check[Either[String, Int]]
  }
  test("Test Either[String, String]") {
    primitiveOrderedBufferSupplier[Either[String, String]]
    check[Either[String, String]]
  }

  test("Test out Option[Option[Int]]") {
    primitiveOrderedBufferSupplier[Option[Option[Int]]]

    check[Option[Option[Int]]]
  }

  test("test product like TestCC") {
    checkMany[(Int, Long, Option[Int], Double, Option[String])]
  }

  test("test specific tuple aa1") {
    primitiveOrderedBufferSupplier[(String, Option[Int], String)]

    checkMany[(String, Option[Int], String)]
  }

  test("test specific tuple 2") {
    check[(String, Option[Int], String)]
  }

  test("test specific tuple 3") {
    val c = List(("", None, ""),
      ("a", Some(1), "b"))
    checkManyExplicit(c.map { i => (i, i) })
  }

  test("Test out TestCC") {
    import TestCC._
    primitiveOrderedBufferSupplier[TestCC]
    check[TestCC]
    checkMany[TestCC]
  }

  test("Test out (Int, Int)") {
    primitiveOrderedBufferSupplier[(Int, Int)]
    check[(Int, Int)]
  }

  test("Test out (String, Option[Int], String)") {
    primitiveOrderedBufferSupplier[(String, Option[Int], String)]
    check[(String, Option[Int], String)]
  }

  test("Test out MyData") {
    import MyData._
    primitiveOrderedBufferSupplier[MyData]
    check[MyData]
  }

  test("Test out MacroOpaqueContainer") {
    // This will test for things which our macros can't view themselves, so need to use an implicit to let the user provide instead.
    // by itself should just work from its own implicits
    implicitly[OrderedSerialization[MacroOpaqueContainer]]

    // Put inside a tuple2 to test that
    primitiveOrderedBufferSupplier[(MacroOpaqueContainer, MacroOpaqueContainer)]
    check[(MacroOpaqueContainer, MacroOpaqueContainer)]
    check[Option[MacroOpaqueContainer]]
    check[List[MacroOpaqueContainer]]
  }
}
