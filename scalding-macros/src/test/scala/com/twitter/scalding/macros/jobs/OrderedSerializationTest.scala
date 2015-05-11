package com.twitter.scalding.macros.jobs

import com.twitter.scalding._
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.typed.TypedPipe
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks

import scala.language.experimental.macros

/**
 * @author Mansur Ashraf.
 */
class OrderedSerializationTest extends FunSuite with PropertyChecks {
  implicit override val generatorDrivenConfig =
    PropertyCheckConfig(minSize = 10, maxSize = 20)

  implicit val arbRecord: Arbitrary[Record] = Arbitrary {
    for {
      a <- Gen.nonEmptyListOf(Gen.alphaNumChar) map (_.mkString)
      b <- Gen.nonEmptyListOf(Gen.alphaNumChar) map (_.mkString)
    } yield Record(a, b)
  }

  implicit val arbRecordContainer: Arbitrary[RecordContainer] = Arbitrary {
    for {
      d <- Gen.choose(0, Int.MaxValue)
      r <- Arbitrary.arbitrary[Record]
    } yield RecordContainer(d, r)
  }

  implicit val arb: Arbitrary[List[RecordContainer]] = Arbitrary {
    Gen.listOfN(100, Arbitrary.arbitrary[RecordContainer]).filter(_.nonEmpty)
  }

  test("Test serialization") {
    forAll(maxSize(10)) { in: List[RecordContainer] =>

      val expected = in.groupBy(r => (r.c, r.record))
        .mapValues(i => i.map(_.c).sum).toList

      val fn = (arg: Args) => new TestJob(in, arg)

      JobTest(fn)
        .arg("output", "output")
        .sink[((Int, Record), Int)](TypedTsv[((Int, Record), Int)]("output")) {
        actual =>
          assert(expected.sortBy { case ((_, x), _) => x } == actual.toList.sortBy { case ((_, x), _) => x })
      }
        .run
    }
  }
}

case class Record(a: String, b: String) extends Ordered[Record] {
  override def compare(that: Record): Int = a.compareTo(that.a) match {
    case 0 => b.compareTo(that.b)
    case x => x
  }
}

case class RecordContainer(c: Int, record: Record)

trait RequiredBinaryComparators extends Job {

  implicit def primitiveOrderedBufferSupplier[T] = macro com.twitter.scalding.macros.impl.OrderedSerializationProviderImpl[T]

  override def config =
    super.config + ("scalding.require.orderedserialization" -> "true")
}

class TestJob(input: List[RecordContainer], args: Args) extends Job(args) with RequiredBinaryComparators {

  assert(implicitly[Ordering[(Int, Record)]].isInstanceOf[Ordering[OrderedSerialization[_]]], "wrong ordering!")

  val pipe1 = TypedPipe.from(input)
    .groupBy(r => (r.c, r.record))

  val pipe2 = TypedPipe.from(input)
    .groupBy(r => (r.c, r.record))

  pipe1.join(pipe2)
    .mapValues { case (left, _) => left.c }
    .sum
    .write(TypedTsv[((Int, Record), Int)](args("output")))
}