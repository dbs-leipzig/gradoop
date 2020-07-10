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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter;

import org.apache.flink.api.java.DataSet;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterAndProjectTemporalEdge;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.List;

/**
 * Filters a set of {@link TemporalEdge} objects based on a specified predicate. Additionally, the
 * operator projects all property values to the output {@link EmbeddingTPGM} that are specified in the
 * given {@code projectionPropertyKeys}.
 * <p>
 * {@code Edge -> Embedding(
 * [IdEntry(SourceId),IdEntry(EdgeId),IdEntry(TargetId)],
 * [PropertyEntry(v1),PropertyEntry(v2)],
 * [EdgeId.tx_from, EdgeId.tx_to, EdgeId.valid_from, EdgeId.valid_to]
 * )}
 * <p>
 * Example:
 * <br>
 * Given a TPGM Edge {@code (0, 1, 2, "friendOf", {since:2017, weight:23}, [123, 1234, 567, 5678])},
 * a predicate {@code "weight = 23"} and
 * list of projection property keys [since,isValid] the operator creates an {@link EmbeddingTPGM}:
 * <br>
 * {@code ([IdEntry(1),IdEntry(0),IdEntry(2)],[PropertyEntry(2017),PropertyEntry(NULL)],
 *    [[123, 1234, 567, 5678]])}
 */
public class FilterAndProjectTemporalEdges implements PhysicalTPGMOperator {
  /**
   * Input graph elements
   */
  private final DataSet<TemporalEdge> input;
  /**
   * Predicates in conjunctive normal form
   */
  private final TemporalCNF predicates;
  /**
   * Property keys used for projection
   */
  private final List<String> projectionPropertyKeys;
  /**
   * Signals that the edge is a loop
   */
  private final boolean isLoop;

  /**
   * Operator name used for Flink operator description
   */
  private String name;

  /**
   * New edge filter operator
   *
   * @param input                  Candidate edges
   * @param predicates             Predicates used to filter edges
   * @param projectionPropertyKeys Property keys used for projection
   * @param isLoop                 is the edge a loop
   */
  public FilterAndProjectTemporalEdges(DataSet<TemporalEdge> input, TemporalCNF predicates,
                                       List<String> projectionPropertyKeys, boolean isLoop) {
    this.input = input;
    this.predicates = predicates;
    this.projectionPropertyKeys = projectionPropertyKeys;
    this.isLoop = isLoop;
    this.setName("FilterAndProjectEdges");
  }

  @Override
  public DataSet<EmbeddingTPGM> evaluate() {
    return input
      .flatMap(new FilterAndProjectTemporalEdge(predicates, projectionPropertyKeys, isLoop))
      .name(getName());
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public void setName(String newName) {
    this.name = newName;
  }
}
