package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }

case class ArgHelperTest(testFn: Seq[DescribedArg] => Unit) extends ArgHelper {
  override def helpRequest(describedArgs: Seq[DescribedArg]): Execution[Unit] = {
    testFn(describedArgs)
    Execution.unit
  }
}

class ArgHelpTest extends WordSpec with Matchers {
  def job = TypedPipe.from(List(1, 2, 3)).toIterableExecution.map(_.foreach(println))

  "ArgHelper" should {
    "print help when asked" in {
      var helpCalled = false
      val helper = ArgHelperTest((describeArgs: Seq[DescribedArg]) => helpCalled = true)

      val args = List(OptionalArg("name", "Name of person"))
      val config = Config.unitTestDefault.setArgs(Args("--help"))

      helper.describe(args, job.unit).waitFor(config, Local(true))
      assert(helpCalled, "Help function was called")
    }
  }

  it should {
    "run job without help" in {
      var helpCalled = false
      val helper = ArgHelperTest((describeArgs: Seq[DescribedArg]) => helpCalled = true)

      val args = List(OptionalArg("name", "Name of person"))
      val config = Config.unitTestDefault.setArgs(Args(""))

      helper.describe(args, job.unit).waitFor(config, Local(true))
      assert(!helpCalled, "Help function was not called")
    }
  }

  it should {
    "call help even when given missing args" in {
      var helpCalled = false
      val helper = ArgHelperTest((describeArgs: Seq[DescribedArg]) => helpCalled = true)

      val args = List(OptionalArg("name", "Name of person"))
      val config = Config.unitTestDefault.setArgs(Args(List("--help", "--name", "Bill", "--phone", "111")))

      helper.validatedDescribe(args, job.unit).waitFor(config, Local(true))
      assert(helpCalled, "Help was output")
    }
  }

  it should {
    "not fail when all args are described" in {
      val args = List(OptionalArg("name", "Name of person"), OptionalArg("phone", "Person's phone"))
      val config = Config.unitTestDefault.setArgs(Args(List("--name", "Bill", "--phone", "111")))

      assert(ArgHelp.validatedDescribe(args, job.unit).waitFor(config, Local(true)).isSuccess)
    }
  }

  it should {
    "fail when all args are not described" in {
      val args = List(OptionalArg("name", "Name of person"), OptionalArg("phone", "Person's phone"))
      val config = Config.unitTestDefault.setArgs(Args(List("--name", "Bill", "--phone", "111", "--address", "123")))

      assert(ArgHelp.validatedDescribe(args, job.unit).waitFor(config, Local(true)).isFailure)
    }
  }
}
