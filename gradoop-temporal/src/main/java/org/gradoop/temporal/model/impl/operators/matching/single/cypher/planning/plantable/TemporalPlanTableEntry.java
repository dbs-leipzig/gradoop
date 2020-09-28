/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.plantable;

import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation.TemporalQueryPlanEstimator;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a query plan and additional meta data. Plan table entries are managed in a
 * {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTable}.
 */
public class TemporalPlanTableEntry extends PlanTableEntry {
  /**
   * The estimator containing the query plan.
   */
  private final TemporalQueryPlanEstimator estimator;
  /**
   * Estimated cardinality of the query result as computed by the estimator.
   */
  private long estimatedCardinality = -1;

  /**
   * Creates a new plan table entry.
   *
   * @param type          the partial match type this entry represents
   * @param processedVars processed query variables
   * @param predicates    predicates not covered by this plan
   * @param estimator     cardinality estimator
   */
  public TemporalPlanTableEntry(Type type, Set<String> processedVars, CNF predicates,
                                TemporalQueryPlanEstimator estimator) {
    super(type, processedVars, predicates, null);
    this.estimator = estimator;
  }

  @Override
  public Set<String> getAllVariables() {
    return new HashSet<>(estimator.getQueryPlan().getRoot().getEmbeddingMetaData()
      .getVariables());
  }

  @Override
  public Set<String> getAttributedVariables() {
    return new HashSet<>(estimator.getQueryPlan().getRoot().getEmbeddingMetaData()
      .getVariablesWithProperties());
  }

  /**
   * Returns all (variable,key) pairs needed for further query processing.
   *
   * @return (variable, key) pairs needed for further processing
   */
  public Set<Pair<String, String>> getProjectionPairs() {
    return getPredicates().getVariables().stream()
      .flatMap(var -> getPredicates().getPropertyKeys(var).stream().map(key -> Pair.of(var, key)))
      .collect(Collectors.toSet());
  }

  @Override
  public long getEstimatedCardinality() {
    if (estimatedCardinality == -1) {
      estimatedCardinality = estimator.getCardinality();
    }
    return estimatedCardinality;
  }

  @Override
  public QueryPlan getQueryPlan() {
    return estimator.getQueryPlan();
  }


}
