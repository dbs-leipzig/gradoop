/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.QueryPlanEstimator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;

import java.util.List;
import java.util.Set;

/**
 * Represents a query plan and additional meta data. Plan table entries are managed in a
 * {@link PlanTable}.
 */
public class PlanTableEntry {
  /**
   * Type of the element a plan entry represents.
   */
  public enum Type {
    /**
     * A single vertex
     */
    VERTEX,
    /**
     * A single edge
     */
    EDGE,
    /**
     * A variable length path
     */
    PATH,
    /**
     * A partial graph contains joined vertices, edges, paths or partial graphs.
     */
    PARTIAL_GRAPH
  }

  /**
   * The type of this entry
   */
  private final Type type;
  /**
   * The variables that are already processed by the query plan.
   */
  private final Set<String> processedVars;
  /**
   * Remaining predicates
   */
  private final CNF predicates;
  /**
   * The estimator containing the query plan.
   */
  private QueryPlanEstimator estimator;

  public PlanTableEntry(Type type, Set<String> processedVars, CNF predicates, QueryPlanEstimator estimator) {
    this.type = type;
    this.processedVars = processedVars;
    this.predicates = predicates;
    this.estimator = estimator;
  }

  /**
   * Returns the type of this plan table entry.
   *
   * @return plan table entry type
   */
  public Type getType() {
    return type;
  }

  public CNF getPredicates() {
    return predicates;
  }

  /**
   * Returns the query variables covered by this plan table entry
   *
   * @return covered variables
   */
  public List<String> getAllVariables() {
    return estimator.getQueryPlan().getRoot().getEmbeddingMetaData().getVariables();
  }

  /**
   * Returns the query variables that are evaluable in a filter operator.
   *
   * @return query variables with assigned properties
   */
  public List<String> getAttributedVariables() {
    return estimator.getQueryPlan().getRoot().getEmbeddingMetaData().getVariablesWithProperties();
  }

  /**
   * Returns the query variables that have been processed by this plan entry.
   *
   * @return processed vars
   */
  public Set<String> getProcessedVariables() {
    return processedVars;
  }

  /**
   * Returns the estimated cardinality of the query plan represented by this entry.
   *
   * @return estimated cardinality
   */
  public long getEstimatedCardinality() {
    return estimator.getCardinality();
  }

  public QueryPlan getQueryPlan() {
    return estimator.getQueryPlan();
  }

  @Override
  public String toString() {
    return String.format("PlanTableEntry | type: %s | all-vars: %s | " +
        "proc-vars: %s | attr-vars: %s | est-card: %d | prediates: %s | Plan :%n%s",
      type, getAllVariables(), getProcessedVariables(), getAttributedVariables(),
      estimator.getCardinality(), predicates, estimator.getQueryPlan());
  }
}