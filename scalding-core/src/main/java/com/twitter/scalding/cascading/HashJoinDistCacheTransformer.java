package com.twitter.scalding;

import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.PathScopeExpression;
import cascading.flow.planner.iso.finder.Match;
import cascading.flow.planner.iso.transformer.ElementFactory;
import cascading.flow.planner.iso.transformer.GraphTransformer;
import cascading.flow.planner.iso.transformer.ReplaceGraphTransformer;
import cascading.flow.planner.iso.transformer.Transformed;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.RuleTransformer;
import cascading.flow.planner.rule.expressiongraph.SyncPipeExpressionGraph;
import cascading.pipe.HashJoin;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.util.TempHfs;

import static cascading.flow.planner.rule.PlanPhase.BalanceAssembly;

class HashJoinExpression extends RuleExpression {

  public HashJoinExpression() {
    super(
      new SyncPipeExpressionGraph(),
      new ExpressionGraph()
        .arc(
          new FlowElementExpression(ElementCapture.Primary, Hfs.class),
          PathScopeExpression.BLOCKING,
          new FlowElementExpression(HashJoin.class)
        )
    );
  }
}

/**
 * Wraps the rhs of any HashJoin in a DistCacheTap.
 */
public class HashJoinDistCacheTransformer extends RuleReplaceFactoryBasedTransformer {
  public HashJoinDistCacheTransformer() {
    super(
      BalanceAssembly,
      new HashJoinExpression(),
      DistCacheEnabledHadoopPlanner.DIST_CACHE_TAP
    );
  }
}

/**
 * GraphTransformer that uses the supplied factory to generate the replacement FlowElement.
 *
 * Note: This only works if exactly one FlowElement is being replaced.
 */
class ReplaceGraphFactoryBasedTransformer extends ReplaceGraphTransformer {

  private final String factoryName;

  public ReplaceGraphFactoryBasedTransformer(
    GraphTransformer graphTransformer,
    ExpressionGraph filter,
    String factoryName) {

    super(graphTransformer, filter);
    this.factoryName = factoryName;
  }

  @Override
  protected boolean transformGraphInPlaceUsing(
    Transformed<ElementGraph> transformed,
    ElementGraph graph,
    Match match) {

    Set<FlowElement> captured = match.getCapturedElements(ElementCapture.Primary);
    if (captured.isEmpty()) {
      return false;
    } else if (captured.size() != 1) {
      throw new IllegalStateException("Expected one, but found multiple flow elements in the match expression: " + captured);
    } else {
      FlowElement replace = captured.iterator().next();
      ElementFactory elementFactory = transformed.getPlannerContext().getElementFactoryFor(factoryName);
      FlowElement replaceWith = elementFactory.create(graph, replace);
      ElementGraphs.replaceElementWith(graph, replace, replaceWith);
      return true;
    }
  }
}

/**
 * RuleTransformer that uses the supplied expression to fetch the FlowElement to replace.
 * The replacement FlowElement is created using the supplied factory.
 */
class RuleReplaceFactoryBasedTransformer extends RuleTransformer {

  private final String factoryName;

  public RuleReplaceFactoryBasedTransformer(
    PlanPhase phase,
    RuleExpression ruleExpression,
    String factoryName) {

    super(phase, ruleExpression);
    this.factoryName = factoryName;

    if (subGraphTransformer != null) {
      graphTransformer = new ReplaceGraphFactoryBasedTransformer(subGraphTransformer,
        ruleExpression.getMatchExpression(), factoryName);
    } else if (contractedTransformer != null) {
      graphTransformer = new ReplaceGraphFactoryBasedTransformer(contractedTransformer,
        ruleExpression.getMatchExpression(), factoryName);
    } else {
      graphTransformer = new ReplaceGraphFactoryBasedTransformer(null,
        ruleExpression.getMatchExpression(), factoryName);
    }
  }
}

