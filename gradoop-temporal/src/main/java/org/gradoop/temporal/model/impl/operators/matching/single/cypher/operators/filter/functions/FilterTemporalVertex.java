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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

/**
 * Filters temporal vertices by a given predicate
 * practically identical to
 * {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterVertex}
 * but adjusted to process TPGM vertices
 */
public class FilterTemporalVertex implements FilterFunction<TemporalVertex> {

  /**
   * Filter predicate
   */
  private final TemporalCNF predicates;

  /**
   * Creates a new UDF
   *
   * @param predicates filter predicates
   */
  public FilterTemporalVertex(TemporalCNF predicates) {
    this.predicates = predicates;
  }

  @Override
  public boolean filter(TemporalVertex vertex) throws Exception {
    return predicates.evaluate(vertex);
  }
}
