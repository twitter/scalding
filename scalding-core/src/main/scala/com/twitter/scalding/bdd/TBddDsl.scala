package com.twitter.scalding.bdd

import cascading.flow.FlowDef
import com.twitter.scalding._
import com.twitter.scalding.source.TypedText
import scala.collection.mutable.Buffer
import TDsl._

trait TBddDsl extends FieldConversions with TypedPipeOperationsConversions {

  def Given[TypeIn](source: TypedTestSource[TypeIn]): TestCaseGiven1[TypeIn] = new TestCaseGiven1[TypeIn](source)

  def GivenSources(sources: List[TypedTestSource[_]]): TestCaseGivenList = new TestCaseGivenList(sources)

  abstract class TypedTestSource[T] {
    def data: Iterable[T]

    def asSource: Source =
      IterableSource(data map { Tuple1(_) }, 'tuple)

    def readFromSourceAsTyped(implicit flowDef: FlowDef, mode: Mode): TypedPipe[T] =
      asSource.read.toTypedPipe[Tuple1[T]]('tuple) map { _._1 }

    def addSourceDataToJobTest(jobTest: JobTest) = jobTest.source(asSource, data)
  }

  class SimpleTypedTestSource[T](val data: Iterable[T]) extends TypedTestSource[T] {
    def addSourceToJob(jobTest: JobTest, source: Source): JobTest =
      jobTest.source[T](source, data)
  }

  implicit def fromSimpleTypeToTypedSource[T](data: Iterable[T]): SimpleTypedTestSource[T] =
    new SimpleTypedTestSource(data)

  case class TestCaseGiven1[TypeIn](source: TypedTestSource[TypeIn]) {
    def And[TypeIn2](other: TypedTestSource[TypeIn2]) = TestCaseGiven2[TypeIn, TypeIn2](source, other)

    def When[TypeOut: Manifest: TupleConverter: TupleSetter](op: OneTypedPipeOperation[TypeIn, TypeOut]): TestCaseWhen[TypeOut] = TestCaseWhen(List(source), op)
  }

  case class TestCaseGiven2[TypeIn1, TypeIn2](source: TypedTestSource[TypeIn1], other: TypedTestSource[TypeIn2]) {
    def And[TypeIn3](third: TypedTestSource[TypeIn3]) = TestCaseGiven3(source, other, third)

    def When[TypeOut: Manifest: TupleConverter: TupleSetter](op: TwoTypedPipesOperation[TypeIn1, TypeIn2, TypeOut]): TestCaseWhen[TypeOut] = TestCaseWhen(List(source, other), op)
  }

  case class TestCaseGiven3[TypeIn1, TypeIn2, TypeIn3](source: TypedTestSource[TypeIn1], other: TypedTestSource[TypeIn2], third: TypedTestSource[TypeIn3]) {
    def And(next: TypedTestSource[_]) = TestCaseGivenList(List(source, other, third, next))

    def When[TypeOut: Manifest: TupleConverter: TupleSetter](op: ThreeTypedPipesOperation[TypeIn1, TypeIn2, TypeIn3, TypeOut]): TestCaseWhen[TypeOut] = TestCaseWhen(List(source, other, third), op)
  }

  case class TestCaseGivenList(sources: List[TypedTestSource[_]]) {
    def And(next: TypedTestSource[_]) = TestCaseGivenList((next :: sources.reverse).reverse)

    def When[TypeOut: Manifest](op: ListOfTypedPipesOperations[TypeOut]): TestCaseWhen[TypeOut] = TestCaseWhen(sources, op)
  }

  case class TestCaseWhen[OutputType: Manifest](sources: List[TypedTestSource[_]], operation: TypedPipeOperation[OutputType]) {
    def Then(assertion: Buffer[OutputType] => Unit): Unit = {
      CompleteTestCase(sources, operation, assertion).run()
    }
  }

  case class CompleteTestCase[OutputType: Manifest](sources: List[TypedTestSource[_]], operation: TypedPipeOperation[OutputType], assertion: Buffer[OutputType] => Unit) {

    class DummyJob(args: Args) extends Job(args) {
      val inputPipes: List[TypedPipe[_]] = sources.map(testSource => testSource.readFromSourceAsTyped)

      val outputPipe = operation(inputPipes)

      implicit val td: TypeDescriptor[OutputType] = new TypeDescriptor[OutputType] {
        def converter = TupleConverter.singleConverter
        def setter = TupleSetter.singleSetter
        def fields = new cascading.tuple.Fields("item")
      }
      outputPipe.write(TypedText.tsv[OutputType]("output"))
    }

    def run(): Unit = {
      val jobTest = JobTest(new DummyJob(_))

      // Add Sources
      sources foreach { _.addSourceDataToJobTest(jobTest) }

      implicit val td: TypeDescriptor[OutputType] = new TypeDescriptor[OutputType] {
        def converter = TupleConverter.singleConverter
        def setter = TupleSetter.singleSetter
        def fields = new cascading.tuple.Fields("item")
      }

      // Add Sink
      jobTest.sink[OutputType](TypedText.tsv[OutputType]("output")) {
        buffer: Buffer[OutputType] => assertion(buffer)
      }

      // Execute
      jobTest.run.finish()
    }
  }

}
