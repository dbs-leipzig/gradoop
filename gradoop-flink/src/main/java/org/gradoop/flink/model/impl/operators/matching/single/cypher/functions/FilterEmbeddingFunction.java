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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.Filter;

import java.util.Map;

/**
 * Filters a set of embedding by given predicates
 */
public class FilterEmbeddingFunction extends RichFilterFunction<Embedding> {
  /**
   * Predicates used for filtering
   */
  private final CNF predicates;
  /**
   * Mapping of variables names to embedding column
   */
  private final Map<String, Integer> columnMapping;

  /**
   * New embedding filter function
   * @param predicates predicates used for filtering
   * @param columnMapping mapping of variable names to embedding column
   */
  public FilterEmbeddingFunction(CNF predicates, Map<String, Integer> columnMapping) {
    this.predicates = predicates;
    this.columnMapping = columnMapping;
  }

  @Override
  public boolean filter(Embedding embedding) {
    return Filter.filter(predicates, embedding, columnMapping);
  }
}
