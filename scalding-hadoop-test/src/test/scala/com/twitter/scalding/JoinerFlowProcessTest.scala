package com.twitter.scalding

import cascading.pipe.joiner.{ JoinerClosure, InnerJoin }
import cascading.tuple.Tuple
import com.twitter.scalding.platform.{ HadoopSharedPlatformTest, HadoopPlatformJobTest, HadoopPlatformTest }
import org.scalatest.{ Matchers, WordSpec }

import java.util.{ Iterator => JIterator }

import org.slf4j.{ LoggerFactory, Logger }

class CheckFlowProcessJoiner(uniqueID: UniqueID) extends InnerJoin {
  override def getIterator(joinerClosure: JoinerClosure): JIterator[Tuple] = {
    println("CheckFlowProcessJoiner.getItertor")

    val flowProcess = RuntimeStats.getFlowProcessForUniqueId(uniqueID)
    if (flowProcess == null) {
      throw new NullPointerException("No active FlowProcess was available.")
    }

    super.getIterator(joinerClosure)
  }
}

class CheckForFlowProcessInFieldsJob(args: Args) extends Job(args) {
  val uniqueID = UniqueID.getIDFor(flowDef)
  val stat = Stat("joins")

  val inA = Tsv("inputA", ('a, 'b))
  val inB = Tsv("inputB", ('x, 'y))

  val p = inA.joinWithSmaller('a -> 'x, inB).map(('b, 'y) -> 'z) { args: (String, String) =>
    stat.inc

    val flowProcess = RuntimeStats.getFlowProcessForUniqueId(uniqueID)
    if (flowProcess == null) {
      throw new NullPointerException("No active FlowProcess was available.")
    }

    s"${args._1},${args._2}"
  }

  p.write(Tsv("output", ('b, 'y)))
}

class CheckForFlowProcessInTypedJob(args: Args) extends Job(args) {
  val uniqueID = UniqueID.getIDFor(flowDef)
  val stat = Stat("joins")

  val inA = TypedPipe.from(TypedTsv[(String, String)]("inputA"))
  val inB = TypedPipe.from(TypedTsv[(String, String)]("inputB"))

  inA.group.join(inB.group).forceToReducers.mapGroup((key, valuesIter) => {
    stat.inc

    val flowProcess = RuntimeStats.getFlowProcessForUniqueId(uniqueID)
    if (flowProcess == null) {
      throw new NullPointerException("No active FlowProcess was available.")
    }

    valuesIter.map({ case (a, b) => s"$a:$b" })
  }).toTypedPipe.write(TypedTsv[(String, String)]("output"))
}

class JoinerFlowProcessTest extends WordSpec with Matchers with HadoopSharedPlatformTest {
  "Methods called from a Joiner" should {
    "have access to a FlowProcess from a join in the Fields-based API" in {
      HadoopPlatformJobTest(new CheckForFlowProcessInFieldsJob(_), cluster)
        .source(TypedTsv[(String, String)]("inputA"), Seq(("1", "alpha"), ("2", "beta")))
        .source(TypedTsv[(String, String)]("inputB"), Seq(("1", "first"), ("2", "second")))
        .sink(TypedTsv[(String, String)]("output")) { _ =>
          // The job will fail with an exception if the FlowProcess is unavailable.
        }
        .inspectCompletedFlow({ flow =>
          flow.getFlowStats.getCounterValue(Stats.ScaldingGroup, "joins") shouldBe 2
        })
        .run
    }

    "have access to a FlowProcess from a join in the Typed API" in {
      HadoopPlatformJobTest(new CheckForFlowProcessInTypedJob(_), cluster)
        .source(TypedTsv[(String, String)]("inputA"), Seq(("1", "alpha"), ("2", "beta")))
        .source(TypedTsv[(String, String)]("inputB"), Seq(("1", "first"), ("2", "second")))
        .sink[(String, String)](TypedTsv[(String, String)]("output")) { _ =>
          // The job will fail with an exception if the FlowProcess is unavailable.
        }
        .inspectCompletedFlow({ flow =>
          flow.getFlowStats.getCounterValue(Stats.ScaldingGroup, "joins") shouldBe 2
        })
        .run
    }
  }
}
