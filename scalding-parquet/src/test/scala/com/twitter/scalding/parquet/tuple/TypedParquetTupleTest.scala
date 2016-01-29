package com.twitter.scalding.parquet.tuple

import com.twitter.scalding.parquet.tuple.macros.Macros._
import com.twitter.scalding.platform.{ PlatformTest, HadoopPlatformJobTest }
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.{ Args, Job, TypedTsv }
import org.scalatest.{ Matchers, WordSpec }
import org.apache.parquet.filter2.predicate.FilterApi.binaryColumn
import org.apache.parquet.filter2.predicate.{ FilterApi, FilterPredicate }
import org.apache.parquet.io.api.Binary

class TypedParquetTupleTest extends WordSpec with Matchers with PlatformTest {
  "TypedParquetTuple" should {

    "read and write correctly" in {
      import com.twitter.scalding.parquet.tuple.TestValues._

      def toMap[T](i: Iterable[T]): Map[T, Int] = i.groupBy(identity).mapValues(_.size)

      HadoopPlatformJobTest(new WriteToTypedParquetTupleJob(_), cluster)
        .arg("output", "output1")
        .sink[SampleClassB](TypedParquet[SampleClassB](Seq("output1"))) {
          toMap(_) shouldBe toMap(values)
        }.run

      HadoopPlatformJobTest(new ReadWithFilterPredicateJob(_), cluster)
        .arg("input", "output1")
        .arg("output", "output2")
        .sink[Boolean]("output2") { toMap(_) shouldBe toMap(values.filter(_.string == "B1").map(_.a.bool)) }
        .run
    }
  }
}

object TestValues {
  val values = Seq(
    SampleClassB("B1", Some(4.0D), SampleClassA(bool = true, 5, 1L, 1.2F, 1), List(1, 2),
      List(SampleClassD(1, "1"), SampleClassD(2, "2")), Set(1D, 2D), Set(SampleClassF(1, 1F)), Map(1 -> "foo")),
    SampleClassB("B2", Some(3.0D), SampleClassA(bool = false, 4, 2L, 2.3F, 2), List(3, 4), Nil, Set(3, 4), Set(),
      Map(2 -> "bar"), Map(SampleClassD(0, "z") -> SampleClassF(0, 3), SampleClassD(0, "y") -> SampleClassF(2, 6))),
    SampleClassB("B3", None, SampleClassA(bool = true, 6, 3L, 3.4F, 3), List(5, 6),
      List(SampleClassD(3, "3"), SampleClassD(4, "4")), Set(5, 6), Set(SampleClassF(2, 2F))),
    SampleClassB("B4", Some(5.0D), SampleClassA(bool = false, 7, 4L, 4.5F, 4), Nil,
      List(SampleClassD(5, "5"), SampleClassD(6, "6")), Set(), Set(SampleClassF(3, 3F), SampleClassF(5, 4F)),
      Map(3 -> "foo2"), Map(SampleClassD(0, "q") -> SampleClassF(4, 3))))
}

case class SampleClassA(bool: Boolean, short: Short, long: Long, float: Float, byte: Byte)

case class SampleClassB(string: String, double: Option[Double], a: SampleClassA, intList: List[Int],
  dList: List[SampleClassD], doubleSet: Set[Double], fSet: Set[SampleClassF], intStringMap: Map[Int, String] = Map(),
  dfMap: Map[SampleClassD, SampleClassF] = Map())

case class SampleClassC(string: String, a: SampleClassA)
case class SampleClassD(x: Int, y: String)
case class SampleClassF(w: Byte, z: Float)

/**
 * Test job write a sequence of sample class values into a typed parquet tuple.
 * To test typed parquet tuple can be used as sink
 */
class WriteToTypedParquetTupleJob(args: Args) extends Job(args) {
  import com.twitter.scalding.parquet.tuple.TestValues._

  val outputPath = args.required("output")

  val sink = TypedParquetSink[SampleClassB](outputPath)
  TypedPipe.from(values).write(sink)
}

/**
 * Test job read from a typed parquet source with filter predicate and push down(SampleClassC takes only part of
 * SampleClassB's data)
 * To test typed parquet tuple can bse used as source and apply filter predicate and push down correctly
 */
class ReadWithFilterPredicateJob(args: Args) extends Job(args) {
  val fp: FilterPredicate = FilterApi.eq(binaryColumn("string"), Binary.fromString("B1"))

  val inputPath = args.required("input")
  val outputPath = args.required("output")

  val input = TypedParquet[SampleClassC](inputPath, fp)

  TypedPipe.from(input).map(_.a.bool).write(TypedTsv[Boolean](outputPath))
}

