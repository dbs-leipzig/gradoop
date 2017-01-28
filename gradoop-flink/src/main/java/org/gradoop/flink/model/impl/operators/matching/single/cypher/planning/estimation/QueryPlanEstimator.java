package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation;

import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.BinaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.LeafNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.UnaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.ExpandEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.JoinEmbeddingsNode;

/**
 * Estimates the cardinality of a given query plan based on statistics about the search graph.
 */
public class QueryPlanEstimator {
  /**
   * The query plan to estimate
   */
  private final QueryPlan queryPlan;
  /**
   * Estimates the cardinality of the joins in the given query plan.
   */
  private final JoinEmbeddingsEstimator joinEstimator;

  /**
   * Estimates the cardinality and selectivity of the leaf nodes.
   */
  private final FilterElementEstimator filterElementEstimator;

  /**
   * Creates a new plan estimator.
   *
   * @param queryPlan query plan
   * @param queryHandler query handler
   * @param graphStatistics graph statistics
   */
  public QueryPlanEstimator(QueryPlan queryPlan, QueryHandler queryHandler,
    GraphStatistics graphStatistics) {
    this.queryPlan = queryPlan;
    this.joinEstimator = new JoinEmbeddingsEstimator(queryHandler, graphStatistics);
    this.filterElementEstimator = new FilterElementEstimator(queryHandler, graphStatistics);
  }

  /**
   * Traverses the query plan and computes the estimated cardinality according to the nodes.
   *
   * @return estimated cardinality of the specified plan
   */
  public long getCardinality() {
    traversePlan(queryPlan.getRoot());

    long cardinality = joinEstimator.getCardinality();
    if (cardinality == 0) {
      // plan contains only a leaf node
      cardinality = filterElementEstimator.getCardinality();
    }
    double selectivity = filterElementEstimator.getSelectivity();

    return Math.round(cardinality * selectivity);
  }

  private void traversePlan(PlanNode node) {
    if (node instanceof JoinEmbeddingsNode) {
      this.joinEstimator.visit((JoinEmbeddingsNode) node);
    }
    if (node instanceof ExpandEmbeddingsNode) {
      this.joinEstimator.visit((ExpandEmbeddingsNode) node);
    }
    if (node instanceof LeafNode) {
      this.filterElementEstimator.visit((LeafNode) node);
    }

    if (node instanceof BinaryNode) {
      traversePlan(((BinaryNode) node).getLeftChild());
      traversePlan(((BinaryNode) node).getRightChild());
    }
    if (node instanceof UnaryNode) {
      traversePlan(((UnaryNode) node).getChildNode());
    }
  }
}
