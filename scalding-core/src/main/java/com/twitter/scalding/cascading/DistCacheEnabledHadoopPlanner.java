package com.twitter.scalding;

import cascading.flow.FlowElement;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.iso.transformer.ElementFactory;
import cascading.flow.planner.rule.transformer.IntermediateTapElementFactory;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.hadoop.planner.HadoopPlanner;
import cascading.pipe.Checkpoint;
import cascading.pipe.Pipe;
import cascading.property.ConfigDef;
import cascading.tap.Tap;
import cascading.tap.hadoop.DistCacheTap;
import cascading.tap.hadoop.Hfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Same as HadoopPlanner, but with DistCacheTap enabled for rhs of HashJoins
 *
 * This is achieved by using a DistCacheTapElementFactory with associated
 * cascading rules to match HashJoin expressions.
 */
public class DistCacheEnabledHadoopPlanner extends HadoopPlanner {

  /* Key used for registering DistCacheTapElementFactory */
  public static String DIST_CACHE_TAP = "cascading.registry.tap.distcache";

  transient private final Logger LOG = LoggerFactory.getLogger(this.getClass());

  @Override
  public void configRuleRegistryDefaults(RuleRegistry ruleRegistry) {
    super.configRuleRegistryDefaults(ruleRegistry);

    LOG.info("Registering DistCacheTapElementFactory rule..");
    ruleRegistry.addDefaultElementFactory(DIST_CACHE_TAP, new DistCacheTapElementFactory());
  }

  // inner class because we want access to the HadoopPlanner methods,
  // similar to Cascading's TempTapElementFactory.
  public class DistCacheTapElementFactory extends IntermediateTapElementFactory {
    transient private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private DistCacheTap wrap(Hfs hfs) {
      LOG.info("Wrapping Tap {} in a DistCacheTap", hfs);
      return new DistCacheTap(hfs);
    }

    // This copies quite a bit of code from private makeTempTap method
    // in cascading.flow.planner.FlowPlanner, which may need to be
    // consolidated if we move this code to cascading-hadoop.
    private DistCacheTap findTapAndWrap(Pipe pipe, ElementGraph elementGraph) {

      if (!(elementGraph instanceof FlowElementGraph)) {
        throw new IllegalStateException("ElementGraph of type FlowElementGraph expected, but found: " + elementGraph);
      }
      FlowElementGraph flowGraph = (FlowElementGraph) elementGraph;

      Tap checkpointTap = flowGraph.getCheckpointsMap().get(pipe.getName());
      if (checkpointTap != null) {
        LOG.info("Found checkpoint: {}, using tap: {}", pipe.getName(), checkpointTap);
      }
      else {
        // only restart from a checkpoint pipe or checkpoint tap below
        if (pipe instanceof Checkpoint) {
          checkpointTap = makeTempTap(checkpointTapRootPath, pipe.getName());
          // mark as an anonymous checkpoint
          checkpointTap.getConfigDef().setProperty(ConfigDef.Mode.DEFAULT, "cascading.checkpoint", "true");
        } else {
          makeTempTap(pipe.getName());
        }
      }

      if (checkpointTap instanceof Hfs) {
        return wrap((Hfs) checkpointTap);
      } else {
        throw new IllegalStateException("Expected checkpointTap of type Hfs, but found: " + checkpointTap);
      }
    }

    @Override
    public FlowElement create(ElementGraph graph, FlowElement flowElement) {
      if (flowElement instanceof Hfs) {
        return wrap((Hfs) flowElement);
      } else if (flowElement instanceof Pipe) {
        return findTapAndWrap((Pipe) flowElement, graph);
      } else {
        throw new IllegalStateException("FlowElement of type Pipe or Hfs expected, but found: " + flowElement);
      }
    }
  }
}

