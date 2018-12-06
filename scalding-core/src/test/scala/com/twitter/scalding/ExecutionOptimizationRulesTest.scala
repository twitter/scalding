package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.scheme.NullScheme
import cascading.tap.Tap
import cascading.tuple.{Fields, Tuple}
import com.stripe.dagon.Rule
import com.twitter.maple.tap.MemorySourceTap
import com.twitter.scalding.typed.TypedPipeGen
import java.io.{InputStream, OutputStream}
import java.util.UUID
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

class ExecutionOptimizationRulesTest extends FunSuite with PropertyChecks {
  class MemorySource[T: TupleConverter](inFields: Fields = Fields.NONE) extends Mappable[T] with TypedSink[T] {
    private[this] val buf = Buffer[Tuple]()
    private[this] val name: String = UUID.randomUUID.toString

    def setter[U <: T] = TupleSetter.asSubSetter(TupleSetter.singleSetter[T])

    override def writeFrom(pipe: Pipe)(implicit flowDef: FlowDef, mode: Mode): Pipe =
      mode match {
        case cl: CascadingLocal =>
          val tap = new MemoryTap(new NullScheme(sinkFields, sinkFields), buf)
          flowDef.addSink(name, tap)
          flowDef.addTail(new Pipe(name, pipe))
          pipe
        case _ => sys.error("MemorySink only usable with cascading local")
      }

    def fields = {
      if (inFields.isNone && setter.arity > 0) {
        Dsl.intFields(0 until setter.arity)
      } else inFields
    }

    override def converter[U >: T]: TupleConverter[U] = TupleConverter.asSuperConverter[T, U](implicitly[TupleConverter[T]])

    private lazy val hdfsTap: Tap[_, _, _] = new MemorySourceTap(buf.asJava, fields)

    override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
      if (readOrWrite == Write) {
        sys.error("IterableSource is a Read-only Source")
      }
      mode match {
        case Local(_) => new MemoryTap[InputStream, OutputStream](new NullScheme(fields, fields), buf)
        case Test(_) => new MemoryTap[InputStream, OutputStream](new NullScheme(fields, fields), buf)
        case Hdfs(_, _) => hdfsTap
        case HadoopTest(_, _) => hdfsTap
        case _ => throw ModeException("Unsupported mode for IterableSource: " + mode.toString)
      }
    }
  }

  val pipe: Gen[Execution[TypedPipe[Int]]] =
    TypedPipeGen.genWithIterableSources.map(pipe => Execution.from(pipe))

  case class PlusOne() extends (TypedPipe[Int] => TypedPipe[Int]) {
    override def apply(p: TypedPipe[Int]): TypedPipe[Int] = p.map(_ + 1)
  }

  case class PlusI(i: Int) extends (TypedPipe[Int] => TypedPipe[Int]) {
    override def apply(p: TypedPipe[Int]): TypedPipe[Int] = p.map(_ + i)
  }

  def mapped(exec: Gen[Execution[TypedPipe[Int]]]): Gen[Execution[TypedPipe[Int]]] = {
    exec.flatMap { pipe =>
      Gen.frequency(
        (1, Execution.Mapped(pipe, PlusOne())),
        (5, Arbitrary.arbitrary[Int].map(i => Execution.Mapped(pipe, PlusI(i))))
      )
    }
  }

  case class ReplaceTo[T](to: Execution[TypedPipe[Int]]) extends (TypedPipe[Int] => Execution[TypedPipe[Int]]) {
    override def apply(v1: TypedPipe[Int]): Execution[TypedPipe[Int]] = to
  }

  def flatMapped(exec: Gen[Execution[TypedPipe[Int]]]): Gen[Execution[TypedPipe[Int]]] = {
    exec.flatMap { from =>
      exec.map { to =>
        from.flatMap(ReplaceTo(to))
      }
    }
  }

  def zipped[A, B](left: Gen[Execution[A]], right: Gen[Execution[B]]): Gen[Execution[(A, B)]] =
    for {
      one <- left
      two <- right
    } yield Execution.Zipped(one, two)

  def write(pipe: Gen[TypedPipe[Int]]): Gen[Execution[TypedPipe[Int]]] =
    pipe.map(_.writeThrough(new MemorySource[Int]()))

  val mappedOrFlatMapped =
    Gen.oneOf(mapped(pipe), flatMapped(pipe))

  val zippedWrites =
    zipped(write(TypedPipeGen.genWithIterableSources), write(TypedPipeGen.genWithIterableSources))

  val mappedWrites =
    mapped(write(TypedPipeGen.genWithIterableSources))

  val zippedFlatMapped =
    Gen.oneOf(
      zipped(flatMapped(pipe), flatMapped(pipe)),
      zipped(mappedWrites, flatMapped(pipe)),
      zipped(flatMapped(pipe), mappedWrites)
    )

  val zippedMapped =
    Gen.oneOf(
      zipped(mappedWrites, mappedOrFlatMapped),
      zipped(mappedOrFlatMapped, mappedWrites)
    )

  val genExec =
    Gen.oneOf(
      zippedWrites,
      zipped(mappedOrFlatMapped, write(TypedPipeGen.genWithIterableSources)),
      zipped(write(TypedPipeGen.genWithIterableSources), mappedOrFlatMapped)
    )

  val iterableExec =
    Gen.oneOf(
      zippedWrites,
      zippedFlatMapped,
      zippedMapped,
      zipped(mappedOrFlatMapped, write(TypedPipeGen.genWithIterableSources)),
      zipped(write(TypedPipeGen.genWithIterableSources), mappedOrFlatMapped)
    ).map { exec =>
      exec flatMap {
        case (left, right) => left.toIterableExecution.zip(right.toIterableExecution)
      } map {
        case (left, right) => left ++ right
      } map {
        _.toList.sorted
      }
    }

  import ExecutionOptimizationRules._

  val allRules = List(
    ZipWrite,
    ZipMap,
    ZipFlatMap,
    MapWrite
  )

  def genRuleFrom(rs: List[Rule[Execution]]): Gen[Rule[Execution]] =
    for {
      c <- Gen.choose(1, rs.size)
      rs <- Gen.pick(c, rs)
    } yield rs.reduce(_.orElse(_))

  val genRule = genRuleFrom(allRules)

  def invert[T](exec: Execution[T]) =
    assert(toLiteral(exec).evaluate == exec)

  test("randomly generated executions trees are invertible") {
    forAll(genExec) { exec =>
      invert(exec)
    }
  }

  test("optimization rules are reproducible") {
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 500)

    forAll(genExec, genRule) { (exec, rule) =>
      val optimized = ExecutionOptimizationRules.apply(exec, rule)
      val optimized2 = ExecutionOptimizationRules.apply(exec, rule)
      assert(optimized == optimized2)
    }
  }

  test("standard rules are reproducible") {
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 500)

    forAll(genExec) { exec =>
      val optimized = ExecutionOptimizationRules.stdOptimizations(exec)
      val optimized2 = ExecutionOptimizationRules.stdOptimizations(exec)
      assert(optimized == optimized2)
    }
  }

  def runAndCompare[A](origin: Execution[A], opt: Execution[A]) = {
    val config = Config.unitTestDefault.setExecutionOptimization(false)

    assert(
      origin.waitFor(config, Local(true)).get ==
        opt.waitFor(config, Local(true)).get
    )
  }

  test("all optimization rules don't change results") {
    forAll(iterableExec, genRule) { (e, r) =>
      val opt = ExecutionOptimizationRules.apply(e, r)
      runAndCompare(e, opt)
    }
  }

  test("zip of writes merged") {
    forAll(zippedWrites) { e =>
      val opt = ExecutionOptimizationRules.apply(e, ZipWrite)

      assert(e.isInstanceOf[Execution.Zipped[_, _]])
      assert(opt.isInstanceOf[Execution.WriteExecution[_]])
    }
  }

  test("push map fn into write") {
    forAll(mappedWrites) { e =>
      val opt = ExecutionOptimizationRules.apply(e, MapWrite)

      assert(e.isInstanceOf[Execution.Mapped[_, _]])
      assert(opt.isInstanceOf[Execution.WriteExecution[_]])
    }
  }

  test("push map into down after zip") {
    forAll(zippedMapped) { e =>
      val opt = ExecutionOptimizationRules.apply(e, ZipMap)

      e match {
        case Execution.Zipped(one: Execution.Mapped[s, t], two) =>
          assert(true)
        case Execution.Zipped(one, two: Execution.Mapped[s, t]) =>
          assert(true)
        case _ =>
          fail(s"$e is not zipped with map")
      }

      opt match {
        case Execution.Zipped(one: Execution.Mapped[s, t], two) =>
          fail(s"$opt didn't push map into zip")
        case Execution.Zipped(one, two: Execution.Mapped[s, t]) =>
          fail(s"$opt didn't push map into zip")
        case _ =>
          assert(true)
      }
    }
  }

  test("push zip into flat map") {
    forAll(zippedFlatMapped) { e =>
      val opt = ExecutionOptimizationRules.apply(e, ZipFlatMap)

      e match {
        case Execution.Zipped(one: Execution.FlatMapped[s, t], two) =>
          assert(true)
        case Execution.Zipped(one, two: Execution.FlatMapped[s, t]) =>
          assert(true)
        case _ =>
          fail(s"$e is not zipped with flat mapped")
      }

      assert(opt.isInstanceOf[Execution.FlatMapped[_, _]])
    }
  }
}
