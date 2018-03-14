package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }

/**
 * Simple identity job that reads from a Tsv and writes to a Tsv with no change.
 *
 * @param args to the job. "input" specifies the input file, and "output" the output file.
 */
class SimpleTestJob(args: Args) extends Job(args) {
  Tsv(args("input")).read.write(Tsv(args("output")))
}

class JobTestTest extends WordSpec with Matchers {
  "A JobTest" should {
    "error helpfully when a source in the job doesn't have a corresponding .source call" in {
      val testInput: List[(String, Int)] = List(("a", 1), ("b", 2))

      // The source required by SimpleTestJob
      val requiredSource = Tsv("input")

      // The incorrect source we will provide, which is different from requiredSource and should
      // cause an error
      val incorrectSource = Tsv("different-input")

      // A method that runs a JobTest where the sources don't match
      def runJobTest() = JobTest(new SimpleTestJob(_))
        .arg("input", "input")
        .arg("output", "output")
        .source(incorrectSource, testInput)
        .sink[(String, Int)](Tsv("output")){ outBuf => { outBuf shouldBe testInput } }
        .run

      the[IllegalArgumentException] thrownBy {
        runJobTest()
      } should have message (s"Failed to create tap for: ${requiredSource}, with error: requirement failed: " + TestTapFactory.sourceNotFoundError.format(requiredSource))
    }
    "use local mode by default" in {
      JobTest(new SimpleTestJob(_)).getTestMode(true, None) match {
        case m: HadoopTest => m.jobConf.get("mapreduce.framework.name") shouldBe "local"
      }
    }
  }
}
