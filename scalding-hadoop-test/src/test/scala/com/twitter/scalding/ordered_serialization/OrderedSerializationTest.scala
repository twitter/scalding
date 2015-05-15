package com.twitter.scalding.ordered_serialization

import com.twitter.scalding._
import com.twitter.scalding.platform.{ HadoopPlatformJobTest, HadoopPlatformTest }
import com.twitter.scalding.serialization.OrderedSerialization

import org.scalacheck.{ Arbitrary, Gen }
import org.scalatest.FunSuite

import scala.language.experimental.macros
import scala.math.Ordering

object OrderedSerializationTest {
  implicit val genASGK = Arbitrary {
    for {
      ts <- Arbitrary.arbitrary[Long]
      b <- Gen.nonEmptyListOf(Gen.alphaNumChar).map (_.mkString)
    } yield NestedCaseClass(RichDate(ts), (b, b))
  }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get
  val data = sample[List[NestedCaseClass]].take(1000)
}

case class NestedCaseClass(day: RichDate, key: (String, String))

class OrderedSerializationTest extends FunSuite with HadoopPlatformTest {
  import OrderedSerializationTest._
  test("A test job with a fork and join, had previously not had boxed serializations on all branches") {
    val fn = (arg: Args) => new ComplexJob(data, arg)
    HadoopPlatformJobTest(fn, cluster)
      .arg("output1", "output1")
      .arg("output2", "output2")
      .sink[String](TypedTsv[String]("output2")) {
        actual => ()
      }.sink[String](TypedTsv[String]("output1")) { x => () }
      .run
  }
}

class ComplexJob(input: List[NestedCaseClass], args: Args) extends Job(args) {
  implicit def primitiveOrderedBufferSupplier[T]: OrderedSerialization[T] = macro com.twitter.scalding.serialization.macros.impl.OrderedSerializationProviderImpl[T]

  val ds1 = TypedPipe.from(input).map(_ -> 1L).distinct.group

  val ds2 = TypedPipe.from(input).map(_ -> 1L).distinct.group

  ds2
    .keys
    .map(s => s.toString)
    .write(TypedTsv[String](args("output1")))

  ds2.join(ds1)
    .values
    .map(_.toString)
    .write(TypedTsv[String](args("output2")))
}

