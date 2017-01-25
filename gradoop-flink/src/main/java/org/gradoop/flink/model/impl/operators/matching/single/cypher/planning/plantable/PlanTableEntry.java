package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable;

import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.Estimator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;

import java.util.List;
import java.util.Set;

public class PlanTableEntry {

  public enum Type {
    VERTEX,
    EDGE,
    PATH
  }

  private Type type;

  private final Set<String> evaluatedVars;

  private QueryPlan queryPlan;

  private Estimator estimator;

  private long estimatedCardinality;

  public PlanTableEntry(Type type, Set<String> evaluatedVars, QueryPlan queryPlan, Estimator estimator) {
    this.type = type;
    this.evaluatedVars = evaluatedVars;
    this.queryPlan = queryPlan;
    this.estimator = estimator;
    this.estimatedCardinality = -1L;
  }

  /**
   * Returns the type of this plan table entry.
   *
   * @return plan table entry type
   */
  public Type getType() {
    return type;
  }

  /**
   * Returns the query variables covered by this plan table entry
   *
   * @return covered variables
   */
  public List<String> getAllVariables() {
    return queryPlan.getRoot().getEmbeddingMetaData().getVariables();
  }

  /**
   * Returns the query variables that are evaluable in a filter operator.
   *
   * @return query variables with assigned properties
   */
  public List<String> getAttributedVariables() {
    return queryPlan.getRoot().getEmbeddingMetaData().getVariablesWithProperties();
  }

  /**
   * Returns the query variables that have been processed by this plan entry.
   *
   * @return processed vars
   */
  public Set<String> getProcessedVariables() {
    return evaluatedVars;
  }

  public long getEstimatedCardinality() {
    if (estimatedCardinality == -1L) {
      estimatedCardinality = estimator.getCardinality();
    }
    return estimatedCardinality;
  }

  public QueryPlan getQueryPlan() {
    return queryPlan;
  }

  @Override
  public String toString() {
    return String.format("PlanTableEntry | type: %s | all-vars: %s | proc-vars: %s | attr-vars: %s | card: %d | Plan :%n%s",
      type, getAllVariables(), getProcessedVariables(), getAttributedVariables(), getEstimatedCardinality(), queryPlan);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PlanTableEntry that = (PlanTableEntry) o;

    return queryPlan.getRoot().getEmbeddingMetaData().getVertexVariables()
      .equals(that.queryPlan.getRoot().getEmbeddingMetaData().getVertexVariables());
  }

  @Override
  public int hashCode() {
    return queryPlan.getRoot().getEmbeddingMetaData().getVertexVariables().hashCode();
  }
}