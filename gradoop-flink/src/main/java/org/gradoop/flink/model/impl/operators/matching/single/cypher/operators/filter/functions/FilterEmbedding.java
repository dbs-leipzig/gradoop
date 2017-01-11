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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos
  .EmbeddingRecord;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos
  .EmbeddingRecordMetaData;

/**
 * Filters a set of embedding by given predicates
 */
public class FilterEmbedding extends RichFilterFunction<EmbeddingRecord> {
  /**
   * Predicates used for filtering
   */
  private final CNF predicates;
  /**
   * Mapping of variables names to embedding column
   */
  private final EmbeddingRecordMetaData metaData;

  /**
   * New embedding filter function
   * @param predicates predicates used for filtering
   * @param metaData mapping of variable names to embedding column
   */
  public FilterEmbedding(CNF predicates,EmbeddingRecordMetaData metaData) {
    this.predicates = predicates;
    this.metaData = metaData;
  }

  @Override
  public boolean filter(EmbeddingRecord embedding) {
    return predicates.evaluate(embedding, metaData);
  }
}
