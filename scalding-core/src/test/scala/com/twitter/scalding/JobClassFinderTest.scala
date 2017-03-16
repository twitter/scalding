package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }

case class C1(a: Int)
case class C2(b: Int)
case class C3(c: Int)
case class C4(d: Int)

trait TraitType {
  val tp2 = TypedPipe.from(List(C4(0), C4(1)))
}

class JobClassFinderExample(args: Args) extends Job(args) with TraitType {
  val tp = TypedPipe.from(List(C1(1), C1(1), C1(2), C1(3), C1(5)))
  val grouped = tp.groupBy(c => C2(c.a))(new Ordering[C2] {
    override def compare(a: C2, b: C2) = b.b - a.b
  })
  private[this] val withTuple = grouped.toList.mapValues(list => C3(list.length))

  withTuple.write(TypedTsv[(C2, C3)](args("output")))
}

class JobClassFinderTest extends WordSpec with Matchers {
  "JobClassFinder" should {
    "Identify and tokenize used case classes" in {
      val job = JobTest(new JobClassFinderExample(_))
        .arg("output", "outputFile")
        .sink[(C2, C3)](TypedTsv[(C2, C3)]("outputFile")){ _: Any => Unit }.initJob(false)
      val tokenizedClasses = Config.tryFrom(job.config).get.getCascadingSerializationTokens.values.toSet
      tokenizedClasses should contain(classOf[C1].getName)
      tokenizedClasses should contain(classOf[C2].getName)
      tokenizedClasses should contain(classOf[C3].getName)
      tokenizedClasses should contain(classOf[C4].getName)
    }
  }
}