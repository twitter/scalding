package com.twitter.scalding.bdd

import com.twitter.scalding._
import scala.collection.mutable.Buffer
import cascading.tuple.Fields
import com.twitter.scalding.Tsv

trait BddDsl extends FieldConversions with PipeOperationsConversions {
  def Given(source: TestSource): TestCaseGiven1 = new TestCaseGiven1(source)

  def Given(sources: List[TestSource]): TestCaseGivenList = new TestCaseGivenList(sources)

  trait TestSourceWithoutSchema {
    def addSourceToJob(jobTest: JobTest, source: Source): JobTest

    def withSchema(schema: Fields) = new TestSource(this, schema)
  }

  class SimpleTypeTestSourceWithoutSchema[T](val data: Iterable[T])(implicit setter: TupleSetter[T]) extends TestSourceWithoutSchema {
    def addSourceToJob(jobTest: JobTest, source: Source): JobTest =
      jobTest.source[T](source, data)(setter)
  }

  implicit def fromSimpleTypeDataToSourceWithoutSchema[T](data: Iterable[T])(implicit setter: TupleSetter[T]): SimpleTypeTestSourceWithoutSchema[T] =
    new SimpleTypeTestSourceWithoutSchema(data)(setter)

  class TestSource(data: TestSourceWithoutSchema, schema: Fields) {
    def sources = List(this)

    def name: String = "Source_" + hashCode

    def asSource: Source = Tsv(name, schema)

    def addSourceDataToJobTest(jobTest: JobTest) = data.addSourceToJob(jobTest, asSource)
  }

  case class TestCaseGiven1(source: TestSource) {
    def And(other: TestSource) = TestCaseGiven2(source, other)

    def When(op: OnePipeOperation): TestCaseWhen = TestCaseWhen(List(source), op)
  }

  case class TestCaseGiven2(source: TestSource, other: TestSource) {
    def And(third: TestSource) = TestCaseGiven3(source, other, third)

    def When(op: TwoPipesOperation): TestCaseWhen = TestCaseWhen(List(source, other), op)
  }

  case class TestCaseGiven3(source: TestSource, other: TestSource, third: TestSource) {
    def And(next: TestSource) = TestCaseGivenList(List(source, other, third, next))

    def When(op: ThreePipesOperation): TestCaseWhen = TestCaseWhen(List(source, other, third), op)
  }

  case class TestCaseGivenList(sources: List[TestSource]) {
    def And(next: TestSource) = TestCaseGivenList((next :: sources.reverse).reverse)

    def When(op: PipeOperation): TestCaseWhen = TestCaseWhen(sources, op)
  }

  case class TestCaseWhen(sources: List[TestSource], operation: PipeOperation) {
    def Then[OutputType](assertion: Buffer[OutputType] => Unit)(implicit conv: TupleConverter[OutputType]): Unit = {
      CompleteTestCase(sources, operation, assertion).run()
    }
  }

  case class CompleteTestCase[OutputType](sources: List[TestSource], operation: PipeOperation, assertion: Buffer[OutputType] => Unit)(implicit conv: TupleConverter[OutputType]) {

    class DummyJob(args: Args) extends Job(args) {
      val inputPipes: List[RichPipe] = sources.map(testSource => RichPipe(testSource.asSource.read))

      val outputPipe = RichPipe(operation(inputPipes))

      outputPipe.write(Tsv("output"))
    }

    def run(): Unit = {
      val jobTest = JobTest(new DummyJob(_))

      // Add Sources
      sources foreach { _.addSourceDataToJobTest(jobTest) }

      // Add Sink
      jobTest.sink[OutputType](Tsv("output")) {
        assertion(_)
      }

      // Execute
      jobTest.run.finish()
    }
  }

}
