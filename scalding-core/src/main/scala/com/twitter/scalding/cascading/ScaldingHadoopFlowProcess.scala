package com.twitter.scalding

import cascading.flow.BaseFlow
import cascading.flow.hadoop.planner.MapReduceHadoopRuleRegistry
import cascading.flow.hadoop.HadoopFlowConnector
import cascading.flow.planner.FlowPlanner
import cascading.flow.planner.rule.RuleRegistrySet

/**
 * Same as MapReduceHadoopRuleRegistry, but with an additional rule
 * to enable DistCacheTap for rhs of HashJoins.
 */
class MapReduceHadoopRuleRegistryWithDistCache extends MapReduceHadoopRuleRegistry {

  if (!addRule(new HashJoinDistCacheTransformer))
    sys.error("Could not add HashJoinTransformer rule to RuleRegistry")
}

trait DistCacheEnabledFlowConnector { self: HadoopFlowConnector =>

  override protected def createDefaultRuleRegistrySet: RuleRegistrySet =
    new RuleRegistrySet(new MapReduceHadoopRuleRegistryWithDistCache)

  override protected def createFlowPlanner: FlowPlanner[_ <: BaseFlow[_], _] =
    new DistCacheEnabledHadoopPlanner
}

/**
 * FlowConnector for Hadoop that enables DistCacheTap for HashJoins.
 *
 * To use this, set: cascading.flow.connector.class=com.twitter.scalding.HadoopFlowConnectorWithDistCache
 */
class HadoopFlowConnectorWithDistCache(m: java.util.Map[Object, Object])
  extends HadoopFlowConnector(m)
  with DistCacheEnabledFlowConnector

