/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable;

import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.QueryPlanEstimator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

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
     * A graph contains joined vertices, edges, paths or graphs.
     */
    GRAPH
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
  /**
   * Estimated cardinality of the query result as computed by the estimator.
   */
  private long estimatedCardinality = -1;

  /**
   * Creates a new plan table entry.
   *
   * @param type the partial match type this entry represents
   * @param processedVars processed query variables
   * @param predicates predicates not covered by this plan
   * @param estimator cardinality estimator
   */
  public PlanTableEntry(Type type, Set<String> processedVars, CNF predicates,
    QueryPlanEstimator estimator) {
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

  /**
   * Returns the remaining predicates not covered by this plan entry.
   *
   * @return remaining predicates
   */
  public CNF getPredicates() {
    return predicates;
  }

  /**
   * Returns the query variables covered by this plan table entry
   *
   * @return covered variables
   */
  public Set<String> getAllVariables() {
    return new HashSet<>(estimator.getQueryPlan().getRoot().getEmbeddingMetaData()
      .getVariables());
  }

  /**
   * Returns the query variables that are evaluable in a filter operator.
   *
   * @return query variables with assigned properties
   */
  public Set<String> getAttributedVariables() {
    return new HashSet<>(estimator.getQueryPlan().getRoot().getEmbeddingMetaData()
      .getVariablesWithProperties());
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
   * Returns all (variable,key) pairs attached to the query plan.
   *
   * @return (variable,key) pairs attached to the current plan
   */
  public Set<Pair<String, String>> getPropertyPairs() {
    return getAttributedVariables().stream()
      .flatMap(var -> getQueryPlan().getRoot().getEmbeddingMetaData().getPropertyKeys(var).stream()
        .map(key -> Pair.of(var, key)))
      .collect(Collectors.toSet());
  }

  /**
   * Returns all (variable,key) pairs needed for further query processing.
   *
   * @return (variable,key) pairs needed for further processing
   */
  public Set<Pair<String, String>> getProjectionPairs() {
    return predicates.getVariables().stream()
      .flatMap(var -> predicates.getPropertyKeys(var).stream().map(key -> Pair.of(var, key)))
      .collect(Collectors.toSet());
  }

  /**
   * Returns the estimated cardinality of the query plan represented by this entry.
   *
   * @return estimated cardinality
   */
  public long getEstimatedCardinality() {
    if (estimatedCardinality == -1) {
      estimatedCardinality = estimator.getCardinality();
    }
    return estimatedCardinality;
  }

  public QueryPlan getQueryPlan() {
    return estimator.getQueryPlan();
  }

  @Override
  public String toString() {
    return String.format("PlanTableEntry | type: %s | all-vars: %s | " +
        "proc-vars: %s | attr-vars: %s | est-card: %d | prediates: %s | Plan :%n%s",
      type, getAllVariables(), getProcessedVariables(), getAttributedVariables(),
      estimatedCardinality, predicates, estimator.getQueryPlan());
  }
}
