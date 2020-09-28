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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.List;

/**
 * Applies a given predicate on a {@link org.gradoop.temporal.model.impl.pojo.TemporalVertex}
 * and projects specified property values to the output embedding.
 */
public class FilterAndProjectTemporalVertex extends
  RichFlatMapFunction<TemporalVertex, Embedding> {
  /**
   * Predicates used for filtering
   */
  private final CNF predicates;
  /**
   * Property keys used for value projection
   */
  private final List<String> projectionPropertyKeys;

  /**
   * New vertex filter function
   *
   * @param predicates             predicates used for filtering
   * @param projectionPropertyKeys property keys that will be used for projection
   */
  public FilterAndProjectTemporalVertex(CNF predicates, List<String> projectionPropertyKeys) {
    this.predicates = predicates;
    this.projectionPropertyKeys = projectionPropertyKeys;
  }

  @Override
  public void flatMap(TemporalVertex vertex, Collector<Embedding> out) throws Exception {
    if (predicates.evaluate(vertex)) {
      out.collect(EmbeddingTPGMFactory.fromVertex(vertex, projectionPropertyKeys));
    }
  }

}
