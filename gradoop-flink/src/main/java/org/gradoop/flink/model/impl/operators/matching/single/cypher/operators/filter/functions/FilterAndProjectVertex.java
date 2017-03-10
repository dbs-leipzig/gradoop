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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingFactory;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;

import java.util.List;

/**
 * Applies a given predicate on a {@link Vertex} and projects specified property values to the
 * output embedding.
 */
public class FilterAndProjectVertex extends RichFlatMapFunction<Vertex, Embedding> {
  /**
   * Predicates used for filtering
   */
  private final CNF predicates;
  /**
   * Property keys used for value projection
   */
  private final List<String> projectionPropertyKeys;
  /**
   * Meta data describing the vertex embedding used for filtering
   */
  private final EmbeddingMetaData filterMetaData;
  /**
   * PropertyKeys of the embedding used for filtering
   */
  private final List<String> filterPropertyKeys;

  /**
   * New vertex filter function
   *
   * @param vertexVariable Variable assigned to the vertex
   * @param predicates predicates used for filtering
   * @param projectionPropertyKeys property keys that will be used for projection
   */
  public FilterAndProjectVertex(String vertexVariable, CNF predicates, List<String>
    projectionPropertyKeys) {
    this.predicates = predicates;
    this.projectionPropertyKeys = projectionPropertyKeys;

    this.filterMetaData = new EmbeddingMetaData();
    this.filterMetaData.setEntryColumn(vertexVariable, EmbeddingMetaData.EntryType.VERTEX, 0);
    int i = 0;
    for (String propertyKey : predicates.getPropertyKeys(vertexVariable)) {
      this.filterMetaData.setPropertyColumn(vertexVariable, propertyKey, i++);
    }

    this.filterPropertyKeys = filterMetaData.getPropertyKeys(vertexVariable);
  }

  @Override
  public void flatMap(Vertex vertex, Collector<Embedding> out) throws Exception {
    if (predicates.evaluate(EmbeddingFactory.fromVertex(vertex, filterPropertyKeys),
      filterMetaData)) {
      out.collect(EmbeddingFactory.fromVertex(vertex, projectionPropertyKeys));
    }
  }
}
