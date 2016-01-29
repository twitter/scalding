package com.twitter.scalding

import cascading.pipe.assembly.AggregateByProps
import com.twitter.scalding.platform.DagwisePlatformTest
import com.twitter.scalding.reducer_estimation.{ RuntimeReducerEstimatorTest, ReducerEstimatorTest, RatioBasedReducerEstimatorTest }
import org.apache.tez.dag.api.TezConfiguration
import cascading.flow.FlowRuntimeProps

// Keeping all of the specifications in the same tests puts the result output all together at the end.
// This is useful given that the Hadoop MiniMRCluster and MiniDFSCluster spew a ton of logging.
class Hadoop2TezFabricTest
  extends DagwisePlatformTest /*with RatioBasedReducerEstimatorTest
  with ReducerEstimatorTest
  with RuntimeReducerEstimatorTest */ {
  /* just realizing here the tests in a Tez context, using cascading-hadoop2-tez */

  override def initialize(): cluster.type = {

    val tempdir = if (Option(System.getProperty("hadoop.tmp.dir")).getOrElse("").isEmpty) "build/test/tmp" else System.getProperty("hadoop.tmp.dir")

    cluster.initialize(Config.empty
      + (TezConfiguration.TEZ_LOCAL_MODE, "true")
      + ("tez.runtime.optimize.local.fetch" -> "true")
      + (TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS -> "3")
      + (TezConfiguration.TEZ_IGNORE_LIB_URIS -> "true")
      + (TezConfiguration.TEZ_GENERATE_DEBUG_ARTIFACTS -> "true")
      + (TezConfiguration.TEZ_AM_SESSION_MODE -> "true") // allows multiple TezClient instances to be used in a single jvm
      + ("hadoop.tmp.dir" -> tempdir)
      + ("mapred.mapper.new-api" -> {
        if (classOf[org.apache.hadoop.mapred.InputFormat[_, _]].isAssignableFrom(classOf[com.twitter.maple.tap.TupleMemoryInputFormat])) "false"
        else if (classOf[org.apache.hadoop.mapreduce.InputFormat[_, _]].isAssignableFrom(classOf[com.twitter.maple.tap.TupleMemoryInputFormat])) "true"
        else ???
      }) /* we are using c.t.maple.tap.MemorySourceTap, which Cascading can't identify as being in the old or new API */
      + (cascading.flow.FlowRuntimeProps.GATHER_PARTITIONS -> "4") /* a value must be provided */ )
  }
}
