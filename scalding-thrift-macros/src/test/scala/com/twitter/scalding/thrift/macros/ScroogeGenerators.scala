package com.twitter.scalding.thrift.macros

import com.twitter.scalding.thrift.macros.scalathrift._
import org.scalacheck.{ Arbitrary, Gen, Prop }
import org.scalacheck.Arbitrary.{ arbitrary => arb }
import java.nio.ByteBuffer
import org.scalacheck.Gen.Parameters
import org.scalacheck.rng.Seed

private object Perturbers {
  def perturb(t0: TestStruct, t1: TestStruct, i: Int): TestStruct = {
    i match {
      case 1 => t0.copy(aString = t1.aString)
      case 2 => t0.copy(aI32 = t1.aI32)
      case 3 => t0.copy(aSecondString = t1.aSecondString)
      case 4 => t0
      case x => sys.error("Can't perturb TestStruct field: " + x)
    }
  }
  def perturb(t0: TestTypes, t1: TestTypes, i: Int): TestTypes = {
    i match {
      case 1 => t0.copy(aBool = t1.aBool)
      case 2 => t0.copy(aByte = t1.aByte)
      case 3 => t0.copy(aI16 = t1.aI16)
      case 4 => t0.copy(aI32 = t1.aI32)
      case 5 => t0.copy(aI64 = t1.aI64)
      case 6 => t0.copy(aDouble = t1.aDouble)
      case 7 => t0.copy(aString = t1.aString)
      case 8 => t0.copy(aEnum = t1.aEnum)
      case 9 => t0.copy(aBinary = t1.aBinary)
      case 10 => t0
      case x => sys.error("Can't perturb TestTypes field: " + x)
    }
  }
  def perturb(t0: TestLists, t1: TestLists, i: Int): TestLists = {
    i match {
      case 1 => t0.copy(aBoolList = t1.aBoolList)
      case 2 => t0.copy(aByteList = t1.aByteList)
      case 3 => t0.copy(aI16List = t1.aI16List)
      case 4 => t0.copy(aI32List = t1.aI32List)
      case 5 => t0.copy(aI64List = t1.aI64List)
      case 6 => t0.copy(aDoubleList = t1.aDoubleList)
      case 7 => t0.copy(aStringList = t1.aStringList)
      case 8 => t0.copy(aStructList = t1.aStructList)
      case 9 => t0.copy(aListList = t1.aListList)
      case 10 => t0.copy(aSetList = t1.aSetList)
      case 11 => t0.copy(aMapList = t1.aMapList)
      case 12 => t0
      case x => sys.error("Can't perturb TestLists field: " + x)
    }
  }
  def perturb(t0: TestSets, t1: TestSets, i: Int): TestSets = {
    i match {
      case 1 => t0.copy(aBoolSet = t1.aBoolSet)
      case 2 => t0.copy(aByteSet = t1.aByteSet)
      case 3 => t0.copy(aI16Set = t1.aI16Set)
      case 4 => t0.copy(aI32Set = t1.aI32Set)
      case 5 => t0.copy(aI64Set = t1.aI64Set)
      case 6 => t0.copy(aDoubleSet = t1.aDoubleSet)
      case 7 => t0.copy(aStringSet = t1.aStringSet)
      case 8 => t0.copy(aStructSet = t1.aStructSet)
      case 9 => t0.copy(aListSet = t1.aListSet)
      case 10 => t0.copy(aSetSet = t1.aSetSet)
      case 11 => t0.copy(aMapSet = t1.aMapSet)
      case 12 => t0
      case x => sys.error("Can't perturb TestSets field: " + x)
    }
  }
  def perturb(t0: TestMaps, t1: TestMaps, i: Int): TestMaps = {
    i match {
      case 1 => t0.copy(aBoolMap = t1.aBoolMap)
      case 2 => t0.copy(aByteMap = t1.aByteMap)
      case 3 => t0.copy(aI16Map = t1.aI16Map)
      case 4 => t0.copy(aI32Map = t1.aI32Map)
      case 5 => t0.copy(aI64Map = t1.aI64Map)
      case 6 => t0.copy(aDoubleMap = t1.aDoubleMap)
      case 7 => t0.copy(aStringMap = t1.aStringMap)
      case 8 => t0.copy(aStructMap = t1.aStructMap)
      case 9 => t0.copy(aListMap = t1.aListMap)
      case 10 => t0.copy(aSetMap = t1.aSetMap)
      case 11 => t0.copy(aMapMap = t1.aMapMap)
      case 12 => t0
      case x => sys.error("Can't perturb TestMaps field: " + x)
    }
  }
}

object ScroogeGenerators {
  import Perturbers._
  def dataProvider[T: Arbitrary](i: Int): T = {
    @annotation.tailrec
    def g(innerI: Int, loops: Int): T = {
      val p = Parameters.default.withSize(2)
      implicitly[Arbitrary[T]].arbitrary(p, Seed(innerI)) match {
        case Some(s) => s
        case None if loops < 5 => g(innerI + 1, loops + 1)
        case None => sys.error("Cannot appear to get Some for this generator.")
      }
    }

    g(i, 0)
  }

  implicit def arbitraryTestStruct: Arbitrary[TestStruct] = Arbitrary {
    for {
      aString <- Gen.alphaStr
      aI32 <- Gen.oneOf(arb[Int].map(Some(_)), Gen.const(None))
      aSecondString <- Gen.alphaStr
    } yield TestStruct(aString, aI32, aSecondString)
  }
  case class TestStructPair(a: TestStruct, b: TestStruct)
  implicit def arbitraryTestStructPair: Arbitrary[TestStructPair] = Arbitrary {
    for {
      a <- arb[TestStruct]
      b <- arb[TestStruct]
      i <- Gen.choose(1, 4)
    } yield TestStructPair(a, perturb(a, b, i))
  }

  implicit def arbitraryTestEnum: Arbitrary[TestEnum] = Arbitrary {
    for {
      aEnum <- Gen.oneOf(TestEnum.Zero, TestEnum.One, TestEnum.Two, TestEnum.Large, TestEnum.Huge)
    } yield aEnum
  }

  implicit def arbitraryTestTypes: Arbitrary[TestTypes] = Arbitrary {
    for {
      aBool <- arb[Boolean]
      aByte <- arb[Byte]
      aI16 <- arb[Short]
      aI32 <- arb[Int]
      aI64 <- arb[Long]
      aDouble <- arb[Double]
      aString <- Gen.alphaStr
      aEnum <- Gen.oneOf(TestEnum.Zero, TestEnum.One, TestEnum.Two, TestEnum.Large, TestEnum.Huge)
      aBinary <- Gen.alphaStr.map(s => ByteBuffer.wrap(s.getBytes("UTF-8")))
    } yield TestTypes(aBool, aByte, aI16, aI32, aI64, aDouble, aString, aEnum, aBinary)
  }

  implicit def arbitraryTestUnion: Arbitrary[TestUnion] = Arbitrary {
    for {
      aStructInUnion <- arb[TestStruct].map(TestUnion.AStruct(_))
      aDoubleSetInUnion <- arb[Set[Double]].map(TestUnion.ADoubleSet(_))
      aUnionRes <- Gen.oneOf(aStructInUnion, aDoubleSetInUnion)
    } yield aUnionRes
  }

  case class TestTypesPair(a: TestTypes, b: TestTypes)
  implicit def arbitraryTestTypesPair: Arbitrary[TestTypesPair] = Arbitrary {
    for {
      a <- arb[TestTypes]
      b <- arb[TestTypes]
      i <- Gen.choose(1, 10)
    } yield TestTypesPair(a, perturb(a, b, i))
  }
  implicit def arbitraryTestLists: Arbitrary[TestLists] = Arbitrary {
    for {
      aBoolList <- Gen.listOf(arb[Boolean])
      aByteList <- Gen.listOf(arb[Byte])
      aI16List <- Gen.listOf(arb[Short])
      aI32List <- Gen.listOf(arb[Int])
      aI64List <- Gen.listOf(arb[Long])
      aDoubleList <- Gen.listOf(arb[Double])
      aStringList <- Gen.listOf(Gen.alphaStr)
      aStructList <- Gen.listOf(arb[TestStruct])
      aListList <- Gen.listOf(Gen.listOf(Gen.alphaStr))
      aSetList <- Gen.listOf(Gen.listOf(Gen.alphaStr).map(_.toSet))
      aMapList <- Gen.listOf(Gen.listOf(arb[(Int, Int)]).map(_.toMap))
    } yield TestLists(aBoolList, aByteList, aI16List, aI32List, aI64List, aDoubleList, aStringList, aStructList, aListList, aSetList, aMapList)
  }
  case class TestListsPair(a: TestLists, b: TestLists)
  implicit def arbitraryTestListsPair: Arbitrary[TestListsPair] = Arbitrary {
    for {
      a <- arb[TestLists]
      b <- arb[TestLists]
      i <- Gen.choose(1, 12)
    } yield TestListsPair(a, perturb(a, b, i))
  }
  implicit def arbitraryTestSets: Arbitrary[TestSets] = Arbitrary {
    for {
      aBoolSet <- Gen.listOf(arb[Boolean]).map(_.toSet)
      aByteSet <- Gen.listOf(arb[Byte]).map(_.toSet)
      aI16Set <- Gen.listOf(arb[Short]).map(_.toSet)
      aI32Set <- Gen.listOf(arb[Int]).map(_.toSet)
      aI64Set <- Gen.listOf(arb[Long]).map(_.toSet)
      aDoubleSet <- Gen.listOf(arb[Double]).map(_.toSet)
      aStringSet <- Gen.listOf(Gen.alphaStr).map(_.toSet)
      aStructSet <- Gen.listOf(arb[TestStruct]).map(_.toSet)
      aListSet <- Gen.listOf(Gen.listOf(Gen.alphaStr).map(l => l.to[collection.Seq])).map(_.to[collection.Set])
      aSetSet <- Gen.listOf(Gen.listOf(Gen.alphaStr).map(l => l.to[collection.Set])).map(_.to[collection.Set])
      aMapSet <- Gen.listOf(Gen.listOf(arb[(Int, Int)]).map(l => l.toMap.asInstanceOf[collection.Map[Int, Int]])).map(_.to[collection.Set])
    } yield TestSets(aBoolSet, aByteSet, aI16Set, aI32Set, aI64Set, aDoubleSet, aStringSet, aStructSet, aListSet, aSetSet, aMapSet)
  }
  case class TestSetsPair(a: TestSets, b: TestSets)
  implicit def arbitraryTestSetsPair: Arbitrary[TestSetsPair] = Arbitrary {
    for {
      a <- arb[TestSets]
      b <- arb[TestSets]
      i <- Gen.choose(1, 12)
    } yield TestSetsPair(a, perturb(a, b, i))
  }
  implicit def arbitraryTestMaps: Arbitrary[TestMaps] = Arbitrary {
    for {
      aBoolMap <- Gen.listOf(arb[(Boolean, String)]).map(_.toMap)
      aByteMap <- Gen.listOf(arb[(Byte, Double)]).map(_.toMap)
      aI16Map <- Gen.listOf(arb[(Short, Long)]).map(_.toMap)
      aI32Map <- Gen.listOf(arb[(Int, Int)]).map(_.toMap)
      aI64Map <- Gen.listOf(arb[(Long, Short)]).map(_.toMap)
      aDoubleMap <- Gen.listOf(arb[(Double, Byte)]).map(_.toMap)
      aStringMap <- Gen.listOf(arb[(String, Boolean)]).map(_.toMap)
      aStructMap <- Gen.listOf(arb[(TestStruct, List[String])]).map(_.toMap)
      aListMap <- Gen.listOf(arb[(List[String], TestStruct)]).map(_.toMap.map { case (k, v) => k.to[collection.Seq] -> v }.asInstanceOf[collection.Map[collection.Seq[String], TestStruct]])
      aSetMap <- Gen.listOf(arb[(Set[String], Set[String])]).map(_.toMap.map { case (k, v) => k.to[collection.Set] -> v.to[collection.Set] }.asInstanceOf[collection.Map[collection.Set[String], collection.Set[String]]])
      aMapMap <- Gen.listOf(arb[(Map[Int, Int], Map[Int, Int])]).map(_.toMap.map { case (k, v) => k.asInstanceOf[collection.Map[Int, Int]] -> v.asInstanceOf[collection.Map[Int, Int]] }.asInstanceOf[collection.Map[collection.Map[Int, Int], collection.Map[Int, Int]]])
    } yield TestMaps(aBoolMap, aByteMap, aI16Map, aI32Map, aI64Map, aDoubleMap, aStringMap, aStructMap, aListMap, aSetMap, aMapMap)
  }
  case class TestMapsPair(a: TestMaps, b: TestMaps)
  implicit def arbitraryTestMapsPair: Arbitrary[TestMapsPair] = Arbitrary {
    for {
      a <- arb[TestMaps]
      b <- arb[TestMaps]
      i <- Gen.choose(1, 12)
    } yield TestMapsPair(a, perturb(a, b, i))
  }
}
