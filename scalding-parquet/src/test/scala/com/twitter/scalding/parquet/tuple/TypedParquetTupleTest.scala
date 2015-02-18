package com.twitter.scalding.parquet.tuple

import java.io.File

import cascading.tuple.{ Fields, TupleEntry, Tuple }
import com.twitter.scalding.platform.{ HadoopPlatformJobTest, HadoopPlatformTest }
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.{ TupleSetter, TupleConverter, Job, Args, TypedTsv }
import parquet.schema.MessageType

import org.scalatest.{ Matchers, WordSpec }

class TypedParquetTupleTest extends WordSpec with Matchers with HadoopPlatformTest {
  "TypedParquetTuple" should {

    "read and write correctly" in {
      import TestValues._
      val tempParquet = java.nio.file.Files.createTempDirectory("parquet_tuple_test_parquet_").toAbsolutePath.toString
      try {
        HadoopPlatformJobTest(new WriteToTypedParquetTupleJob(_), cluster)
          .arg("output", tempParquet)
          .run

        HadoopPlatformJobTest(new ReadFromTypedParquetTupleJob(_), cluster)
          .arg("input", tempParquet)
          .sink[String]("output") { _.toSet shouldBe values.map(_.stringValue).toSet }
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
  val values = Seq(SampleClass("A", 1, 4.0D), SampleClass("B", 2, 3.0D), SampleClass("A", 2, 5.0D))
}

case class SampleClass(stringValue: String, intValue: Int, doubleValue: Double)

/**
 * Test job write a sequence of sample class values into a typed parquet tuple.
 * To test typed parquet tuple can be used as sink
 */
class WriteToTypedParquetTupleJob(args: Args) extends Job(args) {

  import SampleClassDescriptor._
  import TestValues._
  val outputPath = args.required("output")

  val parquetTuple = TypedParquetTuple[SampleClass](Seq(outputPath))
  TypedPipe.from(values).write(parquetTuple)
}

/**
 * Test job read from a typed parquet tuple and write the mapped value into a typed csv sink
 * To test typed parquet tuple can bse used as source and read data correctly
 */
class ReadFromTypedParquetTupleJob(args: Args) extends Job(args) {

  import SampleClassDescriptor._

  val inputPath = args.required("input")

  val parquetTuple = TypedParquetTuple[SampleClass](Seq(inputPath))

  TypedPipe.from(parquetTuple).map(_.stringValue).write(TypedTsv[String]("output"))
}

/**
 * Helper class with tuple related setter and converter +
 * parquet schema using parquet schema generation macro
 */
object SampleClassDescriptor {
  import com.twitter.scalding.parquet.tuple.macros.Macros._
  implicit val valueTupleSetter: TupleSetter[SampleClass] = new TupleSetter[SampleClass] {
    override def apply(value: SampleClass): Tuple = {
      val tuple = new Tuple()
      tuple.addString(value.stringValue)
      tuple.addInteger(value.intValue)
      tuple.addDouble(value.doubleValue)
      tuple
    }

    override def arity: Int = 3
  }

  implicit val valueTupleConverter: TupleConverter[SampleClass] = new TupleConverter[SampleClass] {
    override def apply(te: TupleEntry): SampleClass = {
      val stringValue = te.getString("stringValue")
      val intValue = te.getInteger("intValue")
      val doubleValue = te.getDouble("doubleValue")
      SampleClass(stringValue, intValue, doubleValue)
    }

    override def arity: Int = 3
  }

  implicit val sampleClassFields: Fields = new Fields("stringValue", "intValue", "doubleValue")
  implicit val sampleClassParquetSchema: MessageType = caseClassParquetSchema[SampleClass]
}
