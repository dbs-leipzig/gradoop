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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;

/**
 * Filters an Edge by a given predicate
 */
public class FilterEdge implements FilterFunction<Edge> {

  /**
   * Filter predicate
   */
  private final CNF predicates;

  /**
   * Creates a new UDF
   *
   * @param predicates filter predicates
   */
  public FilterEdge(CNF predicates) {
    this.predicates = predicates;
  }

  @Override
  public boolean filter(Edge edge) throws Exception {
    return predicates.evaluate(edge);
  }
}
