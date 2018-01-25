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

package com.twitter.scalding.serialization.macros
import scala.language.higherKinds
import java.io.{ ByteArrayOutputStream, InputStream }
import java.nio.ByteBuffer

import com.twitter.scalding.serialization.{
  JavaStreamEnrichments,
  Law,
  Law1,
  Law2,
  Law3,
  OrderedSerialization,
  Serialization
}
import org.scalacheck.Arbitrary.{ arbitrary => arb }
import org.scalacheck.{ Arbitrary, Gen, Prop }
import org.scalatest.prop.{ Checkers, PropertyChecks }
import org.scalatest.FunSuite //, ShouldMatchers }
import com.twitter.scalding.some.other.space.space._
import scala.collection.immutable.Queue
import scala.language.experimental.macros
import com.twitter.scalding.serialization.macros.impl.BinaryOrdering

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

  implicit def arbitraryTestCaseClassB: Arbitrary[TestCaseClassB] = Arbitrary {
    for {
      aInt <- arb[Int]
      aLong <- arb[Long]
      aDouble <- arb[Double]
      anOption <- arb[Option[Int]]
      anStrOption <- arb[Option[String]]
    } yield TestCaseClassB(aInt, aLong, anOption, aDouble, anStrOption)
  }

  implicit def arbitraryTestDD: Arbitrary[TestCaseClassD] = Arbitrary {
    for {
      aInt <- arb[Int]
    } yield TestCaseClassD(aInt)
  }

  implicit def arbitraryTestEE: Arbitrary[TestCaseClassE] = Arbitrary {
    for {
      aString <- arb[String]
    } yield TestCaseClassE(aString)
  }

  implicit def arbitraryTestObjectE: Arbitrary[TestObjectE.type] = Arbitrary {
    for {
      e <- Gen.const(TestObjectE)
    } yield e
  }

  implicit def arbitrarySealedTraitTest: Arbitrary[SealedTraitTest] = Arbitrary {
    for {
      cc <- arb[TestCC]
      bb <- arb[TestCaseClassB]
      dd <- arb[TestCaseClassD]
      t <- Gen.oneOf(cc, bb, dd, TestObjectE)
    } yield t
  }

  implicit def arbitraryTestSealedAbstractClass: Arbitrary[TestSealedAbstractClass] = Arbitrary {
    for {
      testSealedAbstractClass <- Gen.oneOf(A, B)
    } yield testSealedAbstractClass
  }

  implicit def arbitraryElementY: Arbitrary[ContainerX.ElementY] = Arbitrary {
    for {
      v <- arb[String]
    } yield ContainerX.ElementY(v)
  }

  implicit def arbitraryElementZ: Arbitrary[ContainerX.ElementZ] = Arbitrary {
    for {
      v <- arb[String]
    } yield ContainerX.ElementZ(v)
  }

  implicit def arbitraryTestCaseHardA: Arbitrary[TestCaseHardA] = Arbitrary {
    for {
      cc <- arb[ContainerX.ElementY]
      bb <- arb[ContainerX.ElementZ]
      o <- arb[String]
      t <- Gen.oneOf(cc, bb)
    } yield TestCaseHardA(t, o)
  }

  implicit def arbitraryTestCaseHardB: Arbitrary[TestCaseHardB] = Arbitrary {
    for {
      o <- arb[String]
      t <- Gen.oneOf(ContainerP.ElementA, ContainerP.ElementB)
    } yield TestCaseHardB(t, o)
  }

}

sealed abstract class TestSealedAbstractClass(val name: Option[String])
case object A extends TestSealedAbstractClass(None)
case object B extends TestSealedAbstractClass(Some("b"))

sealed trait SealedTraitTest
case class TestCC(a: Int,
  b: Long,
  c: Option[Int],
  d: Double,
  e: Option[String],
  f: Option[List[String]],
  aBB: ByteBuffer)
  extends SealedTraitTest

case class TestCaseClassB(a: Int, b: Long, c: Option[Int], d: Double, e: Option[String])
  extends SealedTraitTest

case class TestCaseClassD(a: Int) extends SealedTraitTest

case class TestCaseClassE(a: String) extends AnyVal

case object TestObjectE extends SealedTraitTest

case class TypedParameterCaseClass[A](v: A)

sealed trait BigTrait
case class BigTraitA(a: Int) extends BigTrait
case class BigTraitC(a: Int) extends BigTrait
case class BigTraitD(a: Int) extends BigTrait
case class BigTraitE(a: Int) extends BigTrait
case class BigTraitF(a: Int) extends BigTrait
case class BigTraitG(a: Int) extends BigTrait
case class BigTraitH(a: Int) extends BigTrait
case class BigTraitI(a: Int) extends BigTrait
case class BigTraitJ(a: Int) extends BigTrait
case class BigTraitK(a: Int) extends BigTrait
case class BigTraitL(a: Int) extends BigTrait
case class BigTraitM(a: Int) extends BigTrait
case class BigTraitN(a: Int) extends BigTrait
case class BigTraitO(a: Int) extends BigTrait
case class BigTraitP(a: Int) extends BigTrait
case class BigTraitQ(a: Int) extends BigTrait
case class BigTraitR(a: Int) extends BigTrait
case class BigTraitS(a: Int) extends BigTrait
case class BigTraitT(a: Int) extends BigTrait
case class BigTraitU(a: Int) extends BigTrait
case class BigTraitV(a: Int) extends BigTrait
case class BigTraitW(a: Int) extends BigTrait
case class BigTraitX(a: Int) extends BigTrait
case class BigTraitY(a: Int) extends BigTrait
case class BigTraitZ(a: Int) extends BigTrait

object MyData {
  implicit def arbitraryTestCC: Arbitrary[MyData] = Arbitrary {
    for {
      aInt <- arb[Int]
      anOption <- arb[Option[Long]]
    } yield new MyData(aInt, anOption)
  }
}

class MyData(override val _1: Int, override val _2: Option[Long])
  extends Product2[Int, Option[Long]] {
  override def canEqual(that: Any): Boolean = that match {
    case o: MyData => true
    case _ => false
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: MyData =>
      (o._2, _2) match {
        case (Some(l), Some(r)) => r == l && _1 == o._1
        case (None, None) => _1 == o._1
        case _ => false
      }
    case _ => false
  }

  override def hashCode(): Int = _1.hashCode() * _2.hashCode()

}

object MacroOpaqueContainer {
  import java.io._
  implicit val myContainerOrderedSerializer = new OrderedSerialization[MacroOpaqueContainer] {
    val intOrderedSerialization = BinaryOrdering.ordSer[Int]

    override def hash(s: MacroOpaqueContainer) =
      intOrderedSerialization.hash(s.myField) ^ Int.MaxValue
    override def compare(a: MacroOpaqueContainer, b: MacroOpaqueContainer) =
      intOrderedSerialization.compare(a.myField, b.myField)

    override def read(in: InputStream) =
      intOrderedSerialization.read(in).map(MacroOpaqueContainer(_))

    override def write(b: OutputStream, s: MacroOpaqueContainer) =
      intOrderedSerialization.write(b, s.myField)

    override def compareBinary(lhs: InputStream, rhs: InputStream) =
      intOrderedSerialization.compareBinary(lhs, rhs)
    override val staticSize = Some(4)

    override def dynamicSize(i: MacroOpaqueContainer) = staticSize
  }

  implicit def arbitraryMacroOpaqueContainer: Arbitrary[MacroOpaqueContainer] = Arbitrary {
    for {
      aInt <- arb[Int]
    } yield MacroOpaqueContainer(aInt)
  }

  def apply(d: Int): MacroOpaqueContainer = new MacroOpaqueContainer(d)
}

class MacroOpaqueContainer(val myField: Int) {
  override def hashCode(): Int = myField.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: MacroOpaqueContainer => that.myField == myField
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
class MacroOrderingProperties
  extends FunSuite
  with PropertyChecks
  with BinaryOrdering {
  type SetAlias = Set[Double]

  import ByteBufferArb._
  import Container.arbitraryInnerCaseClass
  import OrderedSerialization.{ compare => oBufCompare }

  def gen[T: Arbitrary]: Gen[T] = implicitly[Arbitrary[T]].arbitrary

  def arbMap[T: Arbitrary, U](fn: T => U): Arbitrary[U] = Arbitrary(gen[T].map(fn))

  def collectionArb[C[_], T: Arbitrary](
    implicit cbf: collection.generic.CanBuildFrom[Nothing, T, C[T]]): Arbitrary[C[T]] =
    Arbitrary {
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
          assert(
            compareMem < 0,
            s"Compare binary: $compareBinary, and compareMem : $compareMem must have the same sign")
        } else if (compareBinary > 0) {
          assert(
            compareMem > 0,
            s"Compare binary: $compareBinary, and compareMem : $compareMem must have the same sign")
        }
    }
  }

  def checkMany[T: Arbitrary](implicit obuf: OrderedSerialization[T]) = forAll { i: List[(T, T)] =>
    checkManyExplicit(i)
  }

  def checkWithInputs[T](a: T, b: T)(implicit obuf: OrderedSerialization[T]): Unit = {
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
    assert(rawCompare(a, b) + rawCompare(b, a) === 0,
      "When adding the raw compares in inverse order they should sum to 0")
    assert(oBufCompare(rta, rtb) === oBufCompare(a, b),
      "Comparing a and b with ordered bufferables compare after a serialization RT")
  }

  def checkAreSame[T](a: T, b: T)(implicit obuf: OrderedSerialization[T]): Unit = {
    val rta = rt(a) // before we do anything ensure these don't throw
    val rtb = rt(b) // before we do anything ensure these don't throw
    assert(oBufCompare(rta, a) === 0, s"A should be equal to itself after an RT -- ${rt(a)}")
    assert(oBufCompare(rtb, b) === 0, s"B should be equal to itself after an RT-- ${rt(b)}")
    assert(oBufCompare(a, b) === 0, "In memory comparasons make sense")
    assert(oBufCompare(b, a) === 0, "In memory comparasons make sense")
    assert(rawCompare(a, b) === 0,
      "When adding the raw compares in inverse order they should sum to 0")
    assert(rawCompare(b, a) === 0,
      "When adding the raw compares in inverse order they should sum to 0")
    assert(oBufCompare(rta, rtb) === 0,
      "Comparing a and b with ordered bufferables compare after a serialization RT")
  }

  def check[T: Arbitrary](implicit obuf: OrderedSerialization[T]) = {
    Checkers.check(LawTester(OrderedSerialization.allLaws))
    forAll(minSuccessful(500)) { (a: T, b: T) =>
      checkWithInputs(a, b)
    }
  }

  def checkCollisions[T: Arbitrary: OrderedSerialization] = {
    val ord = implicitly[OrderedSerialization[T]]

    val arbInput = Gen.containerOfN[Set, T](100, arb[T]).map(_.toList)
    val input = arbInput.sample.get
    val hashes = input.map(ord.hash(_))

    assert(input.distinct.size - hashes.distinct.size <= 3) //generously allow upto 3 collision
  }

  def noOrderedSerialization[T](implicit ev: OrderedSerialization[T] = null) =
    assert(ev === null, "Expected unable to produce OrderedSerialization")

  test("Test out Unit") {
    BinaryOrdering.ordSer[Unit]
    check[Unit]
    checkMany[Unit]
  }
  test("Test out Boolean") {
    BinaryOrdering.ordSer[Boolean]
    check[Boolean]
  }
  test("Test out jl.Boolean") {
    implicit val a = arbMap { b: Boolean =>
      java.lang.Boolean.valueOf(b)
    }
    check[java.lang.Boolean]
  }
  test("Test out Byte") { check[Byte] }
  test("Test out jl.Byte") {
    implicit val a = arbMap { b: Byte =>
      java.lang.Byte.valueOf(b)
    }
    check[java.lang.Byte]
    checkCollisions[java.lang.Byte]
  }
  test("Test out Short") { check[Short] }
  test("Test out jl.Short") {
    implicit val a = arbMap { b: Short =>
      java.lang.Short.valueOf(b)
    }
    check[java.lang.Short]
    checkCollisions[java.lang.Short]
  }
  test("Test out Char") { check[Char] }
  test("Test out jl.Char") {
    implicit val a = arbMap { b: Char =>
      java.lang.Character.valueOf(b)
    }
    check[java.lang.Character]
    checkCollisions[java.lang.Character]
  }
  test("Test out Int") {
    BinaryOrdering.ordSer[Int]
    check[Int]
    checkMany[Int]
    checkCollisions[Int]
  }

  test("Test out AnyVal of String") {
    import TestCC._
    check[TestCaseClassE]
    checkMany[TestCaseClassE]
    checkCollisions[TestCaseClassE]
  }

  test("Test out Tuple of AnyVal's of String") {
    import TestCC._
    BinaryOrdering.ordSer[(TestCaseClassE, TestCaseClassE)]
    check[(TestCaseClassE, TestCaseClassE)]
    checkMany[(TestCaseClassE, TestCaseClassE)]
    checkCollisions[(TestCaseClassE, TestCaseClassE)]
  }

  test("Test out Tuple of TestSealedAbstractClass") {
    import TestCC._
    BinaryOrdering.ordSer[TestSealedAbstractClass]
    check[TestSealedAbstractClass]
    checkMany[TestSealedAbstractClass]
    checkCollisions[TestSealedAbstractClass]
  }

  test("Test out jl.Integer") {
    implicit val a = arbMap { b: Int =>
      java.lang.Integer.valueOf(b)
    }
    check[java.lang.Integer]
    checkCollisions[java.lang.Integer]

  }
  test("Test out Float") { check[Float] }
  test("Test out jl.Float") {
    implicit val a = arbMap { b: Float =>
      java.lang.Float.valueOf(b)
    }
    check[java.lang.Float]
    checkCollisions[java.lang.Float]
  }
  test("Test out Long") { check[Long] }
  test("Test out jl.Long") {
    implicit val a = arbMap { b: Long =>
      java.lang.Long.valueOf(b)
    }
    check[java.lang.Long]
    checkCollisions[java.lang.Long]
  }
  test("Test out Double") { check[Double] }
  test("Test out jl.Double") {
    implicit val a = arbMap { b: Double =>
      java.lang.Double.valueOf(b)
    }
    check[java.lang.Double]
    checkCollisions[java.lang.Double]
  }

  test("Test out String") {
    BinaryOrdering.ordSer[String]

    check[String]
    checkMany[String]
    checkCollisions[String]
  }

  test("Test out ByteBuffer") {
    BinaryOrdering.ordSer[ByteBuffer]
    check[ByteBuffer]
    checkCollisions[ByteBuffer]
  }

  test("Test out List[Float]") {
    BinaryOrdering.ordSer[List[Float]]
    check[List[Float]]
    checkCollisions[List[Float]]
  }
  test("Test out Queue[Int]") {
    implicit val isa = collectionArb[Queue, Int]
    BinaryOrdering.ordSer[Queue[Int]]
    check[Queue[Int]]
    checkCollisions[Queue[Int]]
  }
  test("Test out IndexedSeq[Int]") {
    implicit val isa = collectionArb[IndexedSeq, Int]
    BinaryOrdering.ordSer[IndexedSeq[Int]]
    check[IndexedSeq[Int]]
    checkCollisions[IndexedSeq[Int]]
  }
  test("Test out HashSet[Int]") {
    import scala.collection.immutable.HashSet
    implicit val isa = collectionArb[HashSet, Int]
    BinaryOrdering.ordSer[HashSet[Int]]
    check[HashSet[Int]]
    checkCollisions[HashSet[Int]]
  }
  test("Test out ListSet[Int]") {
    import scala.collection.immutable.ListSet
    implicit val isa = collectionArb[ListSet, Int]
    BinaryOrdering.ordSer[ListSet[Int]]
    check[ListSet[Int]]
    checkCollisions[ListSet[Int]]
  }

  test("Test out List[String]") {
    BinaryOrdering.ordSer[List[String]]
    check[List[String]]
    checkCollisions[List[String]]
  }

  test("Test out List[List[String]]") {
    val oBuf = BinaryOrdering.ordSer[List[List[String]]]
    assert(oBuf.dynamicSize(List(List("sdf"))) === None)
    check[List[List[String]]]
    checkCollisions[List[List[String]]]
  }

  test("Test out List[Int]") {
    BinaryOrdering.ordSer[List[Int]]
    check[List[Int]]
    checkCollisions[List[Int]]
  }

  test("Test out SetAlias") {
    BinaryOrdering.ordSer[SetAlias]
    check[SetAlias]
    checkCollisions[SetAlias]
  }

  test("Container.InnerCaseClass") {
    BinaryOrdering.ordSer[Container.InnerCaseClass]
    check[Container.InnerCaseClass]
    checkCollisions[Container.InnerCaseClass]
  }

  test("Test out Seq[Int]") {
    BinaryOrdering.ordSer[Seq[Int]]
    check[Seq[Int]]
    checkCollisions[Seq[Int]]
  }
  test("Test out scala.collection.Seq[Int]") {
    BinaryOrdering.ordSer[scala.collection.Seq[Int]]
    check[scala.collection.Seq[Int]]
    checkCollisions[scala.collection.Seq[Int]]
  }

  test("Test out Array[Byte]") {
    BinaryOrdering.ordSer[Array[Byte]]
    check[Array[Byte]]
    checkCollisions[Array[Byte]]
  }

  test("Test out Vector[Int]") {
    BinaryOrdering.ordSer[Vector[Int]]
    check[Vector[Int]]
    checkCollisions[Vector[Int]]
  }

  test("Test out Iterable[Int]") {
    BinaryOrdering.ordSer[Iterable[Int]]
    check[Iterable[Int]]
    checkCollisions[Iterable[Int]]
  }

  test("Test out Set[Int]") {
    BinaryOrdering.ordSer[Set[Int]]
    check[Set[Int]]
    checkCollisions[Set[Int]]
  }

  test("Test out Set[Double]") {
    BinaryOrdering.ordSer[Set[Double]]
    check[Set[Double]]
    checkCollisions[Set[Double]]
  }

  test("Test out Map[Long, Set[Int]]") {
    BinaryOrdering.ordSer[Map[Long, Set[Int]]]
    check[Map[Long, Set[Int]]]
    val c = List(Map(9223372036854775807L -> Set[Int]()), Map(-1L -> Set[Int](-2043106012)))
    checkManyExplicit(c.map { i =>
      (i, i)
    })
    checkMany[Map[Long, Set[Int]]]
    checkCollisions[Map[Long, Set[Int]]]
  }

  test("Test out Map[Long, Long]") {
    BinaryOrdering.ordSer[Map[Long, Long]]
    check[Map[Long, Long]]
    checkCollisions[Map[Long, Long]]
  }
  test("Test out HashMap[Long, Long]") {
    import scala.collection.immutable.HashMap
    implicit val isa =
      Arbitrary(implicitly[Arbitrary[List[(Long, Long)]]].arbitrary.map(HashMap(_: _*)))
    BinaryOrdering.ordSer[HashMap[Long, Long]]
    check[HashMap[Long, Long]]
    checkCollisions[HashMap[Long, Long]]
  }
  test("Test out ListMap[Long, Long]") {
    import scala.collection.immutable.ListMap
    implicit val isa =
      Arbitrary(implicitly[Arbitrary[List[(Long, Long)]]].arbitrary.map(ListMap(_: _*)))
    BinaryOrdering.ordSer[ListMap[Long, Long]]
    check[ListMap[Long, Long]]
    checkCollisions[ListMap[Long, Long]]
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

  test("Test known hard String Case") {
    val a = "6"
    val b = "곆"
    val ord = Ordering.String
    assert(rawCompare(a, b) === ord.compare(a, b).signum, "Raw and in memory compares match.")

    val c = List(
      "榴㉕⊟풠湜ᙬ覹ꜻ裧뚐⠂覝쫨塢䇺楠谭픚ᐌ轮뺷Ⱟ洦擄黏著탅ﮓꆋ숷梸傠ァ蹵窥轲闇涡飽ꌳ䝞慙擃",
      "堒凳媨쉏떽㶥⾽샣井ㆠᇗ裉깴辫࠷᤭塈䎙寫㸉ᶴ䰄똇䡷䥞㷗䷱赫懓䷏剆祲ᝯ졑쐯헢鷴ӕ秔㽰ퟡ㏉鶖奚㙰银䮌ᕗ膾买씋썴행䣈丶偝쾕鐗쇊ኋ넥︇瞤䋗噯邧⹆♣ἷ铆玼⪷沕辤ᠥ⥰箼䔄◗",
      "騰쓢堷뛭ᣣﰩ嚲ﲯ㤑ᐜ檊೦⠩奯ᓩ윇롇러ᕰెꡩ璞﫼᭵礀閮䈦椄뾪ɔ믻䖔᪆嬽ﾌ鶬曭꣍ᆏ灖㐸뗋ㆃ녵ퟸ겵晬礙㇩䫓ᘞ昑싨",
      "좃ఱ䨻綛糔唄࿁劸酊᫵橻쩳괊筆ݓ淤숪輡斋靑耜঄骐冠㝑⧠떅漫곡祈䵾ᳺ줵됵↲搸虂㔢Ꝅ芆٠풐쮋炞哙⨗쾄톄멛癔짍避쇜畾㣕剼⫁়╢ꅢ澛氌ᄚ㍠ꃫᛔ匙㜗詇閦單錖⒅瘧崥",
      "獌癚畇")
    checkManyExplicit(c.map { i =>
      (i, i)
    })

    val c2 = List("聸", "")
    checkManyExplicit(c2.map { i =>
      (i, i)
    })
  }

  test("Test out Option[Int]") {
    val oser = BinaryOrdering.ordSer[Option[Int]]

    assert(oser.staticSize === None, "can't get the size statically")
    check[Option[Int]]
    checkMany[Option[Int]]
    checkCollisions[Option[Int]]
  }

  test("Test out Option[String]") {
    BinaryOrdering.ordSer[Option[String]]

    check[Option[String]]
    checkMany[Option[String]]
    checkCollisions[Option[String]]
  }

  test("Test Either[Int, Option[Int]]") {
    val oser = BinaryOrdering.ordSer[Either[Int, Option[Int]]]
    assert(oser.staticSize === None, "can't get the size statically")
    check[Either[Int, Option[Int]]]
    checkCollisions[Either[Int, Option[Int]]]
  }
  test("Test Either[Int, String]") {
    val oser = BinaryOrdering.ordSer[Either[Int, String]]
    assert(oser.staticSize === None, "can't get the size statically")
    assert(
      Some(Serialization.toBytes[Either[Int, String]](Left(1)).length) === oser.dynamicSize(
        Left(1)),
      "serialization size matches dynamic size")
    check[Either[Int, String]]
    checkCollisions[Either[Int, String]]
  }
  test("Test Either[Int, Int]") {
    val oser = BinaryOrdering.ordSer[Either[Int, Int]]
    assert(oser.staticSize === Some(5), "can get the size statically")
    check[Either[Int, Int]]
    checkCollisions[Either[Int, Int]]
  }
  test("Test Either[String, Int]") {
    BinaryOrdering.ordSer[Either[String, Int]]
    check[Either[String, Int]]
    checkCollisions[Either[String, Int]]
  }
  test("Test Either[String, String]") {
    BinaryOrdering.ordSer[Either[String, String]]
    check[Either[String, String]]
    checkCollisions[Either[String, String]]
  }

  test("Test out Option[Option[Int]]") {
    BinaryOrdering.ordSer[Option[Option[Int]]]

    check[Option[Option[Int]]]
    checkCollisions[Option[Option[Int]]]
  }

  test("test product like TestCC") {
    checkMany[(Int, Char, Long, Option[Int], Double, Option[String])]
    checkCollisions[(Int, Char, Long, Option[Int], Double, Option[String])]
  }

  test("test specific tuple aa1") {
    BinaryOrdering.ordSer[(String, Option[Int], String)]

    checkMany[(String, Option[Int], String)]
    checkCollisions[(String, Option[Int], String)]
  }

  test("test specific tuple 2") {
    check[(String, Option[Int], String)]
    checkCollisions[(String, Option[Int], String)]
  }

  test("test specific tuple 3") {
    val c = List(
      ("", None, ""),
      ("a", Some(1), "b"))
    checkManyExplicit(c.map { i =>
      (i, i)
    })
  }

  test("Test out TestCC") {
    import TestCC._
    BinaryOrdering.ordSer[TestCC]
    check[TestCC]
    checkMany[TestCC]
    checkCollisions[TestCC]
  }

  test("Test out Sealed Trait") {
    import TestCC._
    BinaryOrdering.ordSer[SealedTraitTest]
    check[SealedTraitTest]
    checkMany[SealedTraitTest]
    checkCollisions[SealedTraitTest]
  }

  test("Test out Sealed TestCaseHardA") {
    import TestCC._
    BinaryOrdering.ordSer[TestCaseHardA]
    check[TestCaseHardA]
    checkMany[TestCaseHardA]
    checkCollisions[TestCaseHardA]
  }

  test("Test out Sealed TestCaseHardB") {
    import TestCC._

    implicit val v: OrderedSerialization[ContainerP] =
      OrderedSerialization.viaTransform(_.id, ContainerP.fromId)

    BinaryOrdering.ordSer[TestCaseHardB]
    check[TestCaseHardB]
    checkMany[TestCaseHardB]
    checkCollisions[TestCaseHardB]
  }

  test("Test out CaseObject") {
    import TestCC._
    BinaryOrdering.ordSer[TestObjectE.type]
    check[TestObjectE.type]
    checkMany[TestObjectE.type]
  }

  test("Test out (Int, Int)") {
    BinaryOrdering.ordSer[(Int, Int)]
    check[(Int, Int)]
    checkCollisions[(Int, Int)]
  }

  test("Test out (String, Option[Int], String)") {
    BinaryOrdering.ordSer[(String, Option[Int], String)]
    check[(String, Option[Int], String)]
    checkCollisions[(String, Option[Int], String)]
  }

  test("Test out MyData") {
    import MyData._
    BinaryOrdering.ordSer[MyData]
    check[MyData]
    checkCollisions[MyData]
  }

  test("Test out MacroOpaqueContainer") {
    // This will test for things which our macros can't view themselves, so need to use an implicit to let the user provide instead.
    // by itself should just work from its own implicits
    implicitly[OrderedSerialization[MacroOpaqueContainer]]

    // Put inside a tuple2 to test that
    BinaryOrdering.ordSer[(MacroOpaqueContainer, MacroOpaqueContainer)]
    check[(MacroOpaqueContainer, MacroOpaqueContainer)]
    checkCollisions[(MacroOpaqueContainer, MacroOpaqueContainer)]
    check[Option[MacroOpaqueContainer]]
    checkCollisions[Option[MacroOpaqueContainer]]
    check[List[MacroOpaqueContainer]]
    checkCollisions[List[MacroOpaqueContainer]]
  }

  test("Does not produce ordering for large sealed trait") {
    noOrderedSerialization[BigTrait]
  }

  def fn[A](
    implicit or: OrderedSerialization[A]): OrderedSerialization[TypedParameterCaseClass[A]] =
    BinaryOrdering.ordSer[TypedParameterCaseClass[A]]

  test("Test out MacroOpaqueContainer inside a case class as an abstract type") {
    fn[MacroOpaqueContainer]
    BinaryOrdering.ordSer[(MacroOpaqueContainer, MacroOpaqueContainer)]
    ()
  }
}
