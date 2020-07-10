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

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;

/**
 * Filters a set of temporal embeddings by given predicates
 */
public class FilterTemporalEmbedding extends RichFilterFunction<EmbeddingTPGM> {
  /**
   * Predicates used for filtering
   */
  private final TemporalCNF predicates;
  /**
   * Mapping of variables names to embedding column
   */
  private final EmbeddingTPGMMetaData metaData;

  /**
   * New embedding filter function
   *
   * @param predicates predicates used for filtering
   * @param metaData   mapping of variable names to embedding column
   */
  public FilterTemporalEmbedding(TemporalCNF predicates, EmbeddingTPGMMetaData metaData) {
    this.predicates = predicates;
    this.metaData = metaData;
  }

  @Override
  public boolean filter(EmbeddingTPGM embedding) {
    return predicates.evaluate(embedding, metaData);
  }
}
