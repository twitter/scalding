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

package com.twitter.lui.scalding.scrooge

import com.twitter.lui.thrift.thrift_scala.test._
import com.twitter.scalding._
import com.twitter.scalding.parquet.scrooge.FixedPathParquetScrooge
import org.scalatest.WordSpec
import scala.util.{ Try, Success, Failure }

object CompareHelper {
  def traversableOnceCompare(a: TraversableOnce[Any], b: TraversableOnce[Any]) =
    a.toIterator.zip(b.toIterator).zipWithIndex.foreach {
      case ((aE, bE), idx) =>
        Try(recursiveCompare(aE, bE)) match {
          case Success(_) => ()
          case Failure(e) => throw new Exception(s"""
Comparing index ${idx} type: ${aE.getClass}
${e.getMessage}

Reference List:  ${a.mkString(",")}
OtherList:       ${b.mkString(",")}
""")
        }
    }

  def productCompare(a: Product, b: Product): Unit =
    traversableOnceCompare(a.productIterator, b.productIterator)

  def recursiveCompare(unknownA: Any, unknownB: Any): Unit =
    (unknownA, unknownB) match {
      case (a: Product, b: Product) => Try(productCompare(a, b)) match {
        case Success(_) => ()
        case Failure(e) => throw new Exception(s"Comparing type: ${a.getClass}\n${e.getMessage}")
      }
      case (a: TraversableOnce[Any], b: TraversableOnce[Any]) => Try(traversableOnceCompare(a, b)) match {
        case Success(_) => ()
        case Failure(e) => throw new Exception(s"Comparing type: ${a.getClass}\n${e.getMessage}")
      }
      case (a, b) => if (a != b) sys.error(s"Reference $a was not equal to test target $b, of type ${a.getClass}")
    }
}

class IntegrationTests extends WordSpec {
  // only set this to false in local development, allows faster iteration speed
  // by not rebuilding the sample parquet data on every invokation
  val SHOULD_DO_WRITE_STEP = true

  import CompareHelper._
  org.apache.log4j.Logger.getLogger("org.apache.parquet.hadoop.ParquetOutputFormat").setLevel(org.apache.log4j.Level.ERROR)
  org.apache.log4j.Logger.getLogger("org.apache.parquet.hadoop.codec.CodecConfig").setLevel(org.apache.log4j.Level.ERROR)
  org.apache.log4j.Logger.getLogger("org.apache.parquet.hadoop.ColumnChunkPageWriteStore").setLevel(org.apache.log4j.Level.ERROR)
  org.apache.log4j.Logger.getLogger("org.apache.parquet.hadoop.thrift.AbstractThriftWriteSupport").setLevel(org.apache.log4j.Level.ERROR)
  org.apache.log4j.Logger.getLogger("org.apache.parquet.hadoop.ParquetFileReader").setLevel(org.apache.log4j.Level.ERROR)
  org.apache.log4j.Logger.getLogger("org.apache.parquet.Log").setLevel(org.apache.log4j.Level.ERROR)

  java.util.logging.Logger.getLogger("org.apache.parquet.hadoop.ParquetOutputFormat").setLevel(java.util.logging.Level.SEVERE)
  java.util.logging.Logger.getLogger("org.apache.parquet.hadoop.codec.CodecConfig").setLevel(java.util.logging.Level.SEVERE)
  java.util.logging.Logger.getLogger("org.apache.parquet.hadoop.ColumnChunkPageWriteStore").setLevel(java.util.logging.Level.SEVERE)
  java.util.logging.Logger.getLogger("org.apache.parquet.hadoop.thrift.AbstractThriftWriteSupport").setLevel(java.util.logging.Level.SEVERE)
  java.util.logging.Logger.getLogger("org.apache.parquet.hadoop.ParquetFileReader").setLevel(java.util.logging.Level.SEVERE)
  java.util.logging.Logger.getLogger("org.apache.parquet.Log").setLevel(java.util.logging.Level.SEVERE)

  implicit class MappableExt[T](m: Mappable[T]) {
    def iter: Iterator[T] = {
      import com.twitter.scalding._
      implicit val cfg = Config.empty
      implicit val mode = Local(false)
      m.toIterator
    }
  }

  "StringAndBinary Source should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[StringAndBinary](s"/tmp/scalding_StringAndBinary")
    val readTarget = new FPLuiScroogeSource[StringAndBinary](s"/tmp/scalding_StringAndBinary")

    val expected: List[StringAndBinary] = (0 until 100).map { i =>
      val str = i.toString
      val bytes = java.nio.ByteBuffer.wrap(str.getBytes("UTF-8"))
      StringAndBinary(str, bytes)
    }.toList

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      expected
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList
    val actual = readTarget.iter.map { t =>
      t.hashCode
      t
    }.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList

    val iter1 = expectedWithFilter.iterator
    val iter2 = actual.iterator
    while (iter1.hasNext) {
      assert(iter1.next == iter2.next)
    }
  }

  "RequiredPrimitiveFixture Source should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[RequiredPrimitiveFixture](s"/tmp/scalding_RequiredPrimitiveFixture")
    val readTarget = new FPLuiScroogeSource[RequiredPrimitiveFixture](s"/tmp/scalding_RequiredPrimitiveFixture")

    val expected: List[RequiredPrimitiveFixture] = (0 until 100).map { i =>
      val boolean = (i % 3 == 0)
      val byte = i.toByte
      val short = i.toShort
      val integer = i
      val long = i.toLong
      val double = i.toDouble
      val str = i.toString
      RequiredPrimitiveFixture(boolean, byte, short, integer, long, double, str)
    }.toList

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      expected
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList
    val actual = readTarget.iter.map { t =>
      t.hashCode
      t
    }.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList

    val iter1 = expectedWithFilter.iterator
    val iter2 = actual.iterator
    while (iter1.hasNext) {
      assert(iter1.next == iter2.next)
    }
  }

  "StructWithOptionalInnerStructAndOptionalBooleanInside should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[StructWithOptionalInnerStructAndOptionalBooleanInside](s"/tmp/scalding_StructWithOptionalInnerStructAndBooleanInside")
    val readTarget = new FPLuiScroogeSource[StructWithOptionalInnerStructAndOptionalBooleanInside](s"/tmp/scalding_StructWithOptionalInnerStructAndBooleanInside")

    val expected: List[StructWithOptionalInnerStructAndOptionalBooleanInside] = (0 until 100).map { i =>
      if (i % 2 == 4)
        StructWithOptionalInnerStructAndOptionalBooleanInside(None)
      else {
        if (i % 2 == 0) {
          StructWithOptionalInnerStructAndOptionalBooleanInside(Some(MaybeBoolean(i.toLong, None)))
        } else {
          StructWithOptionalInnerStructAndOptionalBooleanInside(Some(MaybeBoolean(i.toLong, Some(i % 3 == 0))))
        }

      }
    }.toList

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      expected
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.filter(_._2 % 5 == 0).map(_._1)
    val actual = readTarget.iter.map { t =>
      t.hashCode
      t
    }.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList

    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

  "StructWithOptionalInnerStruct should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[StructWithOptionalInnerStruct](s"/tmp/scalding_StructWithOptionalInnerStruct")
    val readTarget = new FPLuiScroogeSource[StructWithOptionalInnerStruct](s"/tmp/scalding_StructWithOptionalInnerStruct")

    val expected: List[StructWithOptionalInnerStruct] = (0 until 100).map { i =>
      if (i % 2 == 0)
        StructWithOptionalInnerStruct(None)
      else {
        StructWithOptionalInnerStruct(Some(Phone(i + ".. mobile", "work.." + i)))
      }
    }.toList

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      expected
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.filter(_._2 % 5 == 0).map(_._1)
    val actual = readTarget.iter.map { t =>
      t.hashCode
      t
    }.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList

    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

  "PrimitiveMap should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[PrimitiveMap](s"/tmp/scalding_PrimitiveMap")
    val readTarget = new FPLuiScroogeSource[PrimitiveMap](s"/tmp/scalding_PrimitiveMap")

    val expected: List[PrimitiveMap] = (0 until 100).map { i =>
      val booleanMap = (0 until i + 2).map { idx => (idx % 3 == 0) -> (idx % 3 == 0) }.toMap
      val byteMap = (0 until i + 2).map { idx => idx.toByte -> idx.toByte }.toMap
      val shortMap = (0 until i + 2).map { idx => idx.toShort -> idx.toShort }.toMap
      val integerMap = (0 until i + 2).map { idx => idx -> idx }.toMap
      val longMap = (0 until i + 2).map { idx => idx.toLong -> idx.toLong }.toMap
      val doubleMap = (0 until i + 2).map { idx => idx.toDouble -> idx.toDouble }.toMap
      val stringMap = (0 until i + 2).map { idx => idx.toString -> idx.toString }.toMap
      PrimitiveMap(booleanMap, byteMap, shortMap, integerMap, longMap, doubleMap, stringMap)
    }.toList

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      expected
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.filter(_._2 % 5 == 0).map(_._1)
    val actual = readTarget.iter.map { t =>
      t.hashCode
      t
    }.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList

    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        recursiveCompare(expected, actual)
    }

    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

  "PrimitiveList should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[PrimitiveList](s"/tmp/scalding_PrimitiveList")
    val readTarget = new FPLuiScroogeSource[PrimitiveList](s"/tmp/scalding_PrimitiveList")

    val expected: List[PrimitiveList] = (0 until 100).map { i =>
      val booleanList = (0 until i + 2).map { idx => (idx % 3 == 0) }
      val byteList = (0 until i + 2).map { idx => idx.toByte }
      val shortList = (0 until i + 2).map { idx => idx.toShort }
      val integerList = (0 until i + 2).map { idx => idx }
      val longList = (0 until i + 2).map { idx => idx.toLong }
      val doubleList = (0 until i + 2).map { idx => idx.toDouble }
      val stringList = (0 until i + 2).map { idx => idx.toString }
      PrimitiveList(booleanList, byteList, shortList, integerList, longList, doubleList, stringList)
    }.toList

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      expected
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.filter(_._2 % 5 == 0).map(_._1)
    val actual = readTarget.iter.map { t =>
      t.hashCode
      t
    }.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList

    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

  "PrimitiveSet should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[PrimitiveSet](s"/tmp/scalding_PrimitiveSet")
    val readTarget = new FPLuiScroogeSource[PrimitiveSet](s"/tmp/scalding_PrimitiveSet")

    val expected: List[PrimitiveSet] = (0 until 100).map { i =>
      val booleanSet = (0 until i + 2).map { idx => (idx % 3 == 0) }.toSet
      val byteSet = (0 until i + 2).map { idx => idx.toByte }.toSet
      val shortSet = (0 until i + 2).map { idx => idx.toShort }.toSet
      val integerSet = (0 until i + 2).map { idx => idx }.toSet
      val longSet = (0 until i + 2).map { idx => idx.toLong }.toSet
      val doubleSet = (0 until i + 2).map { idx => idx.toDouble }.toSet
      val stringSet = (0 until i + 2).map { idx => idx.toString }.toSet
      PrimitiveSet(booleanSet, byteSet, shortSet, integerSet, longSet, doubleSet, stringSet)
    }.toList

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      expected
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.filter(_._2 % 5 == 0).map(_._1)
    val actual = readTarget.iter.map { t =>
      t.hashCode
      t
    }.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList

    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

  // Simple as it can be, has just 2 fields
  "Phone Source should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[Phone](s"/tmp/scalding_phone")
    val readTarget = new FPLuiScroogeSource[Phone](s"/tmp/scalding_phone")

    val expected: List[Phone] = (0 until 100).map { i =>
      Phone(i + ".. mobile", "work.." + i)
    }.toList

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      (0 until 100).map { i =>
        Phone(i + ".. mobile", "work.." + i)
      }.toList
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.filter(_._2 % 5 == 0).map(_._1)
    val actual = readTarget.iter.map { t =>
      t.hashCode
      t
    }.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList

    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

  // This handles nested structs
  "AStructWithAStruct Source should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[AStructWithAStruct](s"/tmp/scalding_AStructWithAStruct")
    val readTarget = new FPLuiScroogeSource[AStructWithAStruct](s"/tmp/scalding_AStructWithAStruct")

    val expected = (0 until 100).map { i =>
      AStructWithAStruct(AString(i.toString))
    }.toList

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      (0 until 100).map { i =>
        AStructWithAStruct(AString(i.toString))
      }.toList

    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.filter(_._2 % 5 == 0).map(_._1)
    val actual = readTarget.iter.map { t =>
      t.hashCode
      t
    }.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList
    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

  // This handles nested structs
  "RequiredListFixture Source should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[RequiredListFixture](s"/tmp/scalding_RequiredListFixture")
    val readTarget = new FPLuiScroogeSource[RequiredListFixture](s"/tmp/scalding_RequiredListFixture")

    val expected = (0 until 100).map { i =>
      val info = if (i % 2 == 0) Some(i.toString + "info") else None
      val lst = (0 until i + 2).map { idx => Name("Major loop level: " + i.toString + ",minor:" + idx, Some("Major loop level: " + i.toString + "\t" + idx.toString)) }
      RequiredListFixture(info, List(NameList(lst)))
    }.toIterator

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      (0 until 100).map { i =>
        val info = if (i % 2 == 0) Some(i.toString + "info") else None
        val lst = (0 until i + 2).map { idx => Name("Major loop level: " + i.toString + ",minor:" + idx, Some("Major loop level: " + i.toString + "\t" + idx.toString)) }
        RequiredListFixture(info, List(NameList(lst)))
      }.toList

    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList
    val actual = readTarget.iter.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList
    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

  // This RequiredMapFixture nested structs
  "RequiredMapFixture Source should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[RequiredMapFixture](s"/tmp/scalding_RequiredMapFixture")
    val readTarget = new FPLuiScroogeSource[RequiredMapFixture](s"/tmp/scalding_RequiredMapFixture")

    val expected = (0 until 100).map { i =>
      val info = if (i % 2 == 0) Some(i.toString + "info") else None
      val mapD = (0 until i + 2).map{ idx =>
        s"${i}_$idx" -> s"${i}_$idx"
      }.toMap
      RequiredMapFixture(info, mapD)
    }.toIterator

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      expected
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList
    val actual = readTarget.iter.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList
    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

  // This RequiredMapFixture nested structs
  "RequiredEnumMapFixture Source should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[RequiredEnumMapFixture](s"/tmp/scalding_RequiredEnumMapFixture")
    val readTarget = new FPLuiScroogeSource[RequiredEnumMapFixture](s"/tmp/scalding_RequiredEnumMapFixture")

    val expected = (0 until 100).map { i =>
      val info = if (i % 2 == 0) Some(i.toString + "info") else None
      val mapD: Map[Operation, String] = (0 until i + 2).map{ idx =>
        val requiredEnum = (idx % 4) match {
          case 0 => Operation.Add
          case 1 => Operation.Subtract
          case 2 => Operation.Multiply
          case 3 => Operation.Divide
        }
        requiredEnum -> s"${i}_$idx"
      }.toMap
      RequiredEnumMapFixture(info, mapD)
    }.toIterator

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      expected
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList
    val actual = readTarget.iter.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList
    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

  // This handles nested structs
  "TestFieldOfEnum Source should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[TestFieldOfEnum](s"/tmp/scalding_TestFieldOfEnum")
    val readTarget = new FPLuiScroogeSource[TestFieldOfEnum](s"/tmp/scalding_TestFieldOfEnum")

    val expected: List[TestFieldOfEnum] = (0 until 100).map { i =>
      val requiredEnum = (i % 4) match {
        case 0 => Operation.Add
        case 1 => Operation.Subtract
        case 2 => Operation.Multiply
        case 3 => Operation.Divide
      }

      val optionalEnum = ((i + 1) % 4) match {
        case 0 => Operation.Add
        case 1 => Operation.Subtract
        case 2 => Operation.Multiply
        case 3 => Operation.Divide
      }
      TestFieldOfEnum(requiredEnum, Some(optionalEnum))

    }.toList

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      expected
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList
    val actual = readTarget.iter.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toList
    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

  "TestCaseNestedLists Source should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[TestCaseNestedLists](s"/tmp/scalding_TestCaseNestedLists")
    val readTarget = new FPLuiScroogeSource[TestCaseNestedLists](s"/tmp/scalding_TestCaseNestedLists")

    val expected: List[TestCaseNestedLists] = (0 until 50).map { i =>
      val evtList = (0 until i).map { idx =>
        val entries = (0 until idx).map { ydx =>
          val primitiveList = (0 until ydx).map { jdx =>
            jdx.toLong
          }
          if (ydx % 3 == 0) EntryAB(idx, None) else EntryAB(idx, Some(primitiveList))
        }
        if (idx % 2 == 0) EvtDetails(entries) else EvtDetails(null)
      }
      if (i % 3 == 0) TestCaseNestedLists(Some(evtList)) else TestCaseNestedLists(None)
    }.toList

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      expected
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.zipWithIndex.toList //.filter(_._2 % 5 == 0).map(_._1).toList
    val actual = readTarget.iter.zipWithIndex.toList //filter(_._2 % 5 == 0).map(_._1).toList
    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        recursiveCompare(expected, actual)
    }
    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

  "TestUnion Source should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[TestUnion](s"/tmp/scalding_TestUnion")
    val readTarget = new FPLuiScroogeSource[TestUnion](s"/tmp/scalding_TestUnion")

    val expected: List[TestUnion] = (0 until 50).map { i =>
      val person = TestPerson(Name(i.toString), Some(i), null, null)
      val mapComplex = TestMapComplex(Map())
      if (i % 2 == 0) {
        TestUnion.FirstPerson(person)
      } else TestUnion.SecondMap(mapComplex)
    }.toList

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      expected
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.toIterator.map(_.hashCode).toList
    val actual = readTarget.iter.map(_.hashCode).toList
    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        recursiveCompare(expected, actual)
    }
    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

  "SimpleListList Source should round trip" should {
    val writeTarget = new FixedPathParquetScrooge[SimpleListList](s"/tmp/scalding_SimpleListList")
    val readTarget = new FPLuiScroogeSource[SimpleListList](s"/tmp/scalding_SimpleListList")

    val expected: List[SimpleListList] = (0 until 50).map { i =>
      SimpleListList((0 until i + 2).map { j =>
        (0 until j + 2).map { k =>
          s"$i $j $k"
        }
      })
    }.toList

    val exec = TypedPipe.from(List(1)).flatMap{ _ =>
      expected
    }.writeExecution(writeTarget)

    if (SHOULD_DO_WRITE_STEP)
      exec.waitFor(Config.empty, Hdfs(false, new org.apache.hadoop.conf.Configuration))

    // Do a modulo so we handle some skipping, touch some fields to make sure we keep all the columns in sync too
    val expectedWithFilter = expected.toIterator.map(_.hashCode).toList
    val actual = readTarget.iter.map(_.hashCode).toList
    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        recursiveCompare(expected, actual)
    }
    expectedWithFilter.zip(actual).foreach {
      case (expected, actual) =>
        assert(expected == actual)
    }
  }

}

