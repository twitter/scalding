package com.twitter.scalding.parquet.tuple

import java.io.File

import com.twitter.scalding.parquet.tuple.macros.Macros
import com.twitter.scalding.parquet.tuple.scheme.{ ParquetTupleConverter, ParquetReadSupport, ParquetWriteSupport }
import com.twitter.scalding.platform.{ HadoopPlatformJobTest, HadoopPlatformTest }
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.{ Args, Job, TypedTsv }
import org.scalatest.{ Matchers, WordSpec }
import parquet.filter2.predicate.FilterApi.binaryColumn
import parquet.filter2.predicate.{ FilterApi, FilterPredicate }
import parquet.io.api.{ RecordConsumer, Binary }
import parquet.schema.MessageType

class TypedParquetTupleTest extends WordSpec with Matchers with HadoopPlatformTest {
  "TypedParquetTuple" should {

    "read and write correctly" in {
      import com.twitter.scalding.parquet.tuple.TestValues._
      val tempParquet = java.nio.file.Files.createTempDirectory("parquet_tuple_test_parquet_").toAbsolutePath.toString
      try {
        HadoopPlatformJobTest(new WriteToTypedParquetTupleJob(_), cluster)
          .arg("output", tempParquet)
          .run

        HadoopPlatformJobTest(new ReadFromTypedParquetTupleJob(_), cluster)
          .arg("input", tempParquet)
          .sink[Float]("output") { _.toSet shouldBe values.map(_.a.float).toSet }
          .run

        HadoopPlatformJobTest(new ReadWithFilterPredicateJob(_), cluster)
          .arg("input", tempParquet)
          .sink[Boolean]("output") { _.toSet shouldBe values.filter(_.string == "B1").map(_.a.bool).toSet }
          .run

      } finally {
        deletePath(tempParquet)
      }
    }
  }

  def deletePath(path: String) = {
    val dir = new File(path)
    for {
      files <- Option(dir.listFiles)
      file <- files
    } file.delete()
    dir.delete()
  }
}

object TestValues {
  val values = Seq(SampleClassB("B1", 1, Some(4.0D), SampleClassA(bool = true, 5, 1L, 1.2F, 1)),
    SampleClassB("B2", 3, Some(3.0D), SampleClassA(bool = false, 4, 2L, 2.3F, 2)),
    SampleClassB("B3", 9, None, SampleClassA(bool = true, 6, 3L, 3.4F, 3)),
    SampleClassB("B4", 8, Some(5.0D), SampleClassA(bool = false, 7, 4L, 4.5F, 4)))
}

case class SampleClassA(bool: Boolean, short: Short, long: Long, float: Float, byte: Byte)
case class SampleClassB(string: String, int: Int, double: Option[Double], a: SampleClassA)
case class SampleClassC(string: String, a: SampleClassA)

object SampleClassB {
  val schema: String = Macros.caseClassParquetSchema[SampleClassB]
}

class BReadSupport extends ParquetReadSupport[SampleClassB] {
  override val tupleConverter: ParquetTupleConverter[SampleClassB] = Macros.caseClassParquetTupleConverter[SampleClassB]
  override val rootSchema: String = SampleClassB.schema
}

class CReadSupport extends ParquetReadSupport[SampleClassC] {
  override val tupleConverter: ParquetTupleConverter[SampleClassC] = Macros.caseClassParquetTupleConverter[SampleClassC]
  override val rootSchema: String = Macros.caseClassParquetSchema[SampleClassC]
}

class WriteSupport extends ParquetWriteSupport[SampleClassB] {
  override val rootSchema: String = SampleClassB.schema
  override def writeRecord(r: SampleClassB, rc: RecordConsumer, schema: MessageType): Unit =
    Macros.caseClassWriteSupport[SampleClassB](r, rc, schema)
}

/**
 * Test job write a sequence of sample class values into a typed parquet tuple.
 * To test typed parquet tuple can be used as sink
 */
class WriteToTypedParquetTupleJob(args: Args) extends Job(args) {
  import com.twitter.scalding.parquet.tuple.TestValues._

  val outputPath = args.required("output")

  val sink = TypedParquetSink[SampleClassB, WriteSupport](Seq(outputPath))
  TypedPipe.from(values).write(sink)
}

/**
 * Test job read from a typed parquet tuple and write the mapped value into a typed tsv sink
 * To test typed parquet tuple can bse used as source and read data correctly
 */
class ReadFromTypedParquetTupleJob(args: Args) extends Job(args) {

  val inputPath = args.required("input")

  val input = TypedParquet[SampleClassB, BReadSupport](Seq(inputPath))

  TypedPipe.from(input).map(_.a.float).write(TypedTsv[Float]("output"))
}

/**
 * Test job read from a typed parquet source with filter predicate and push down(SampleClassC takes only part of
 * SampleClassB's data)
 * To test typed parquet tuple can bse used as source and apply filter predicate and push down correctly
 */
class ReadWithFilterPredicateJob(args: Args) extends Job(args) {
  val fp: FilterPredicate = FilterApi.eq(binaryColumn("string"), Binary.fromString("B1"))

  val inputPath = args.required("input")

  val input = TypedParquet[SampleClassC, CReadSupport](Seq(inputPath), Some(fp))

  TypedPipe.from(input).map(_.a.bool).write(TypedTsv[Boolean]("output"))
}
