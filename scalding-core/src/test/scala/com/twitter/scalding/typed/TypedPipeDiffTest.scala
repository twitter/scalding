package com.twitter.scalding.typed

import java.io.File
import java.nio.file.Files

import com.twitter.algebird.MapAlgebra
import com.twitter.scalding.{ Config, Local }
import org.scalacheck.{ Arbitrary, Prop }
import org.scalatest.prop.{ Checkers, PropertyChecks }
import org.scalatest.{ FunSuite, PropSpec }

import scala.reflect.ClassTag

class NoOrdering(val x: String) {

  override def equals(other: Any): Boolean = other match {
    case that: NoOrdering => x.equals(that.x)
    case _ => false
  }

  override def hashCode(): Int = x.hashCode
}

class NoOrderingHashCollisions(val x: String) {

  override def equals(other: Any): Boolean = other match {
    case that: NoOrderingHashCollisions => x.equals(that.x)
    case _ => false
  }

  override def hashCode(): Int = 0
}

object TypedPipeRunner {
  def runToList[T](output: TypedPipe[T]): List[T] =
    output
      .toIterableExecution
      .waitFor(Config.default, Local(strictSources = true))
      .get
      .toList
}

class TypedPipeDiffTest extends FunSuite {
  import com.twitter.scalding.typed.TypedPipeRunner._

  val left = List("hi", "hi", "bye", "foo", "bar")
  val right = List("hi", "bye", "foo", "baz")
  val expectedSortedDiff = List(("bar", (1, 0)), ("baz", (0, 1)), ("hi", (2, 1))).sorted

  val leftArr = List(
    Array[Byte](3, 3, 5, 3, 2),
    Array[Byte](2, 2, 2),
    Array[Byte](0, 1, 0))

  val rightArr = List(
    Array[Byte](2, 2, 2),
    Array[Byte](2, 2, 2),
    Array[Byte](3, 3, 5, 3, 2),
    Array[Byte](0, 1, 1))

  val expectedSortedArrDiff = List(
    (Array[Byte](0, 1, 0).toSeq, (1, 0)),
    (Array[Byte](0, 1, 1).toSeq, (0, 1)),
    (Array[Byte](2, 2, 2).toSeq, (1, 2)))

  test("diff works for objects with ordering and good hashcodes") {
    val pipe1 = TypedPipe.from(left)
    val pipe2 = TypedPipe.from(right)
    val diff = TypedPipeDiff.diff(pipe1, pipe2)

    assert(expectedSortedDiff === runToList(diff.toTypedPipe).sorted)
  }

  // this lets us sort the results,
  // without bringing an ordering into scope
  private def sort(x: List[(Seq[Byte], (Long, Long))]): List[(Seq[Byte], (Long, Long))] = {
    import scala.Ordering.Implicits.seqDerivedOrdering
    x.sorted
  }

  test("diffArrayPipes works for arrays") {
    val pipe1 = TypedPipe.from(leftArr)
    val pipe2 = TypedPipe.from(rightArr)

    val diff = TypedPipeDiff.diffArrayPipes(pipe1, pipe2).map { case (arr, counts) => (arr.toSeq, counts) }

    assert(expectedSortedArrDiff === sort(runToList(diff)))
  }

  test("diffWithoutOrdering works for objects with ordering and good hashcodes") {
    val pipe1 = TypedPipe.from(left)
    val pipe2 = TypedPipe.from(right)
    val diff = TypedPipeDiff.diffByHashCode(pipe1, pipe2)

    assert(expectedSortedDiff === runToList(diff).sorted)
  }

  test("diffWithoutOrdering does not require ordering") {
    val pipe1 = TypedPipe.from(left.map(new NoOrdering(_)))
    val pipe2 = TypedPipe.from(right.map(new NoOrdering(_)))
    val diff = TypedPipeDiff.diffByHashCode(pipe1, pipe2)

    assert(expectedSortedDiff === runToList(diff).map { case (nord, counts) => (nord.x, counts) }.sorted)
  }

  test("diffWithoutOrdering works even with hash collisions") {
    val pipe1 = TypedPipe.from(left.map(new NoOrderingHashCollisions(_)))
    val pipe2 = TypedPipe.from(right.map(new NoOrderingHashCollisions(_)))
    val diff = TypedPipeDiff.diffByHashCode(pipe1, pipe2)
    assert(expectedSortedDiff === runToList(diff).map { case (nord, counts) => (nord.x, counts) }.sorted)
  }

  test("diffArrayPipesWithoutOrdering works for arrays of objects with no ordering") {
    val pipe1 = TypedPipe.from(leftArr.map { arr => arr.map { b => new NoOrdering(b.toString) } })
    val pipe2 = TypedPipe.from(rightArr.map { arr => arr.map { b => new NoOrdering(b.toString) } })
    val diff = TypedPipeDiff.diffArrayPipes(pipe1, pipe2)

    assert(expectedSortedArrDiff === sort(runToList(diff).map{ case (arr, counts) => (arr.map(_.x.toByte).toSeq, counts) }))
  }

}

object TypedPipeDiffLaws {
  import com.twitter.scalding.typed.TypedPipeDiff.Enrichments._
  import com.twitter.scalding.typed.TypedPipeRunner._

  def checkDiff[T](left: List[T], right: List[T], diff: List[(T, (Long, Long))]): Boolean = {
    val noDuplicates = diff.size == diff.map(_._1).toSet.size
    val expected = MapAlgebra.sumByKey(left.map((_, (1L, 0L))).iterator ++ right.map((_, (0L, 1L))).iterator)
      .filter { case (t, (rCount, lCount)) => rCount != lCount }

    noDuplicates && expected == diff.toMap
  }

  def checkArrayDiff[T](left: List[Array[T]], right: List[Array[T]], diff: List[(Seq[T], (Long, Long))]): Boolean = {
    checkDiff(left.map(_.toSeq), right.map(_.toSeq), diff)
  }

  def diffLaw[T: Ordering: Arbitrary]: Prop = Prop.forAll { (left: List[T], right: List[T]) =>
    val diff = runToList(TypedPipe.from(left).diff(TypedPipe.from(right)).toTypedPipe)
    checkDiff(left, right, diff)
  }

  def diffArrayLaw[T](implicit arb: Arbitrary[List[Array[T]]], ct: ClassTag[T]): Prop = Prop.forAll { (left: List[Array[T]], right: List[Array[T]]) =>
    val diff = runToList(TypedPipe.from(left).diffArrayPipes(TypedPipe.from(right)))
      .map { case (arr, counts) => (arr.toSeq, counts) }
    checkArrayDiff(left, right, diff)
  }

  def diffByGroupLaw[T: Arbitrary]: Prop = Prop.forAll { (left: List[T], right: List[T]) =>
    val diff = runToList(TypedPipe.from(left).diffByHashCode(TypedPipe.from(right)))
    checkDiff(left, right, diff)
  }

}

class TypedPipeDiffLaws extends PropSpec with PropertyChecks with Checkers {
  override implicit val generatorDrivenConfig = PropertyCheckConfig(minSuccessful = 5)

  property("diffLaws") {
    check(TypedPipeDiffLaws.diffLaw[Int])
    check(TypedPipeDiffLaws.diffLaw[String])
  }

  property("diffArrayLaws") {

    implicit val arbNoOrdering = Arbitrary {
      for {
        strs <- Arbitrary.arbitrary[Array[String]]
      } yield {
        strs.map { new NoOrdering(_) }
      }
    }

    implicit val arbNoOrderingHashCollision = Arbitrary {
      for {
        strs <- Arbitrary.arbitrary[Array[String]]
      } yield {
        strs.map { new NoOrderingHashCollisions(_) }
      }
    }

    check(TypedPipeDiffLaws.diffArrayLaw[Long])
    check(TypedPipeDiffLaws.diffArrayLaw[Int])
    check(TypedPipeDiffLaws.diffArrayLaw[Short])
    check(TypedPipeDiffLaws.diffArrayLaw[Char])
    check(TypedPipeDiffLaws.diffArrayLaw[Byte])
    check(TypedPipeDiffLaws.diffArrayLaw[Boolean])
    check(TypedPipeDiffLaws.diffArrayLaw[Float])
    check(TypedPipeDiffLaws.diffArrayLaw[Double])
    check(TypedPipeDiffLaws.diffArrayLaw[String])
    check(TypedPipeDiffLaws.diffArrayLaw[NoOrdering])
    check(TypedPipeDiffLaws.diffArrayLaw[NoOrderingHashCollisions])
  }

  property("diffByGroupLaws") {

    implicit val arbNoOrdering = Arbitrary {
      for {
        name <- Arbitrary.arbitrary[String]
      } yield {
        new NoOrdering(name)
      }
    }

    implicit val arbNoOrderingHashCollision = Arbitrary {
      for {
        name <- Arbitrary.arbitrary[String]
      } yield {
        new NoOrderingHashCollisions(name)
      }
    }

    check(TypedPipeDiffLaws.diffByGroupLaw[Int])
    check(TypedPipeDiffLaws.diffByGroupLaw[String])
    check(TypedPipeDiffLaws.diffByGroupLaw[NoOrdering])
    check(TypedPipeDiffLaws.diffByGroupLaw[NoOrderingHashCollisions])
  }

}