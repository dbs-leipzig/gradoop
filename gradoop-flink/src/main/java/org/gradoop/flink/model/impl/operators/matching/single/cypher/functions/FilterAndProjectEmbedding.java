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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.Filter;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.Projector;

import java.util.List;
import java.util.Map;

/**
 * Filters a set of embeddings and projects the remaining elements.
 */
public class FilterAndProjectEmbedding extends RichFlatMapFunction<Embedding, Embedding> {
  /**
   * Predicate used for filtering in CNF
   */
  private final CNF predicates;
  /**
   * Holds a list of property keys for every embedding entry
   * The specified properties will be kept in the projection
   */
  private final Map<Integer, List<String>> propertyKeyMapping;
  /**
   * Maps the predicates variables to embedding entries
   */
  private final Map<String, Integer> columnMapping;

  /**
   * Create a new embedding filter and project function
   * @param predicates filter predicates
   * @param propertyKeyMapping maps embedding entries to property keys
   * @param columnMapping maps predicate variables to embedding entries
   */
  public FilterAndProjectEmbedding(CNF predicates,
    Map<Integer, List<String>> propertyKeyMapping, Map<String, Integer> columnMapping) {
    this.predicates = predicates;
    this.propertyKeyMapping = propertyKeyMapping;
    this.columnMapping = columnMapping;
  }

  @Override
  public void flatMap(Embedding embedding, Collector<Embedding> out) throws Exception {
    if (Filter.filter(predicates, embedding, columnMapping)) {
      Projector.project(embedding, propertyKeyMapping);
      out.collect(embedding);
    }
  }
}
