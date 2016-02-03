package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }

case class ArgHelperTest(testFn: Seq[DescribedArg] => Unit) extends ArgHelper {
  override def helpRequest[T](describedArgs: Seq[DescribedArg]): Nothing = {
    testFn(describedArgs)
    throw new HelpException()
  }
}

class ArgHelpTest extends WordSpec with Matchers {
  def job = TypedPipe.from(List(1, 2, 3)).toIterableExecution

  "ArgHelper" should {
    "print help when asked" in {
      var helpCalled = false
      val helper = ArgHelperTest((describeArgs: Seq[DescribedArg]) => helpCalled = true)

      val args = List(OptionalArg("name", "Name of person"))
      val config = Config.unitTestDefault.setArgs(Args("--help"))

      intercept[HelpException] {
        helper.describe(args, job).waitFor(config, Local(true)).get
      }
      assert(helpCalled, "Help function was called")
    }
  }

  it should {
    "run job without help" in {
      var helpCalled = false
      val helper = ArgHelperTest((describeArgs: Seq[DescribedArg]) => helpCalled = true)

      val args = List(OptionalArg("name", "Name of person"))
      val config = Config.unitTestDefault.setArgs(Args(""))

      val returnValues = helper.describe(args, job).waitFor(config, Local(true)).get.toList
      assert(!helpCalled, "Help function was not called")
      assert(returnValues == List(1, 2, 3))
    }
  }

  it should {
    "call help even when given missing args" in {
      var helpCalled = false
      val helper = ArgHelperTest((describeArgs: Seq[DescribedArg]) => helpCalled = true)

      val args = List(OptionalArg("name", "Name of person"))
      val config = Config.unitTestDefault.setArgs(Args(List("--help", "--name", "Bill", "--phone", "111")))

      intercept[HelpException] {
        helper.validatedDescribe(args, job).waitFor(config, Local(true)).get
      }
      assert(helpCalled, "Help was output")
    }
  }

  it should {
    "not fail when all args are described" in {
      val args = List(OptionalArg("name", "Name of person"), OptionalArg("phone", "Person's phone"))
      val config = Config.unitTestDefault.setArgs(Args(List("--name", "Bill", "--phone", "111")))

      val returnValues = ArgHelp.validatedDescribe(args, job).waitFor(config, Local(true)).get
      assert(returnValues == List(1, 2, 3))
    }
  }

  it should {
    "fail when all args are not described" in {
      val args = List(OptionalArg("name", "Name of person"), OptionalArg("phone", "Person's phone"))
      val config = Config.unitTestDefault.setArgs(Args(List("--name", "Bill", "--phone", "111", "--address", "123")))

      intercept[DescriptionValidationException] {
        ArgHelp.validatedDescribe(args, job.unit).waitFor(config, Local(true)).get
      }
    }
  }
}
