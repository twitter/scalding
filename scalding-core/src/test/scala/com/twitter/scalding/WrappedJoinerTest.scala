package com.twitter.scalding

import cascading.flow.FlowException
import cascading.pipe.CoGroup
import cascading.pipe.joiner.{ JoinerClosure, InnerJoin }
import cascading.tuple.Tuple
import org.scalatest.{ Matchers, WordSpec }

import java.util.{ Iterator => JIterator }

class CheckFlowProcessJoiner(uniqueID: UniqueID) extends InnerJoin {
  override def getIterator(joinerClosure: JoinerClosure): JIterator[Tuple] = {
    val flowProcess = RuntimeStats.getFlowProcessForUniqueId(uniqueID)
    if (flowProcess == null) {
      throw new NullPointerException("No active FlowProcess was available.")
    }

    super.getIterator(joinerClosure)
  }
}

class TestWrappedJoinerJob(args: Args) extends Job(args) {
  val uniqueID = UniqueID.getIDFor(flowDef)

  val inA = Tsv("inputA", ('a, 'b))
  val inB = Tsv("inputB", ('x, 'y))

  val joiner = {
    val checkJoiner = new CheckFlowProcessJoiner(uniqueID)
    if (args.boolean("wrapJoiner")) WrappedJoiner(checkJoiner) else checkJoiner
  }

  val p1 = new CoGroup(inA, 'a, inB, 'x, joiner)

  // The .forceToDisk is necessary to have the test work properly.
  p1.forceToDisk.write(Tsv("output"))
}

class WrappedJoinerTest extends WordSpec with Matchers {
  "Methods called from a Joiner" should {
    "have access to a FlowProcess when WrappedJoiner is used" in {
      JobTest(new TestWrappedJoinerJob(_))
        .arg("wrapJoiner", "true")
        .source(Tsv("inputA"), Seq(("1", "alpha"), ("2", "beta")))
        .source(Tsv("inputB"), Seq(("1", "first"), ("2", "second")))
        .sink[(Int, String)](Tsv("output")) { outBuf =>
          // The job will fail with an exception if the FlowProcess is unavailable.
        }
        .runHadoop
        .finish()
    }

    "have no access to a FlowProcess when WrappedJoiner is not used" in {
      try {
        JobTest(new TestWrappedJoinerJob(_))
          .source(Tsv("inputA"), Seq(("1", "alpha"), ("2", "beta")))
          .source(Tsv("inputB"), Seq(("1", "first"), ("2", "second")))
          .sink[(Int, String)](Tsv("output")) { outBuf =>
            // The job will fail with an exception if the FlowProcess is unavailable.
          }
          .runHadoop
          .finish()

        fail("The test Job without WrappedJoiner should fail.")
      } catch {
        case ex: FlowException =>
          ex.getCause.getMessage should include ("the FlowProcess for unique id")
      }
    }
  }
}
