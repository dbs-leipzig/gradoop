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

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

/**
 * Filters a set of embedding by given predicates
 */
public class FilterEmbedding extends RichFilterFunction<Embedding> {
  /**
   * Predicates used for filtering
   */
  private final CNF predicates;
  /**
   * Mapping of variables names to embedding column
   */
  private final EmbeddingMetaData metaData;

  /**
   * New embedding filter function
   *
   * @param predicates predicates used for filtering
   * @param metaData mapping of variable names to embedding column
   */
  public FilterEmbedding(CNF predicates, EmbeddingMetaData metaData) {
    this.predicates = predicates;
    this.metaData = metaData;
  }

  @Override
  public boolean filter(Embedding embedding) {
    return predicates.evaluate(embedding, metaData);
  }
}
