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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingFactory;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;

import java.util.List;

/**
 * Applies a given predicate on a {@link Edge} and projects specified property values to the
 * output embedding.
 */
public class FilterAndProjectEdge extends RichFlatMapFunction<Edge, Embedding> {
  /**
   * Predicates used for filtering
   */
  private final CNF predicates;
  /**
   * Property Keys used for the projection
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
   * New edge filter function
   *
   * @param edgeVariable variable assigned to the edge
   * @param predicates predicates used for filtering
   * @param projectionPropertyKeys property keys that will be used for projection
   */
  public FilterAndProjectEdge(String edgeVariable, CNF predicates,
    List<String> projectionPropertyKeys) {
    this.predicates = predicates;
    this.projectionPropertyKeys = projectionPropertyKeys;

    this.filterMetaData = new EmbeddingMetaData();
    this.filterMetaData.setEntryColumn(edgeVariable, EmbeddingMetaData.EntryType.EDGE, 1);
    int i = 0;
    for (String propertyKey : predicates.getPropertyKeys(edgeVariable)) {
      this.filterMetaData.setPropertyColumn(edgeVariable, propertyKey, i++);
    }

    this.filterPropertyKeys = filterMetaData.getPropertyKeys(edgeVariable);
  }

  @Override
  public void flatMap(Edge edge, Collector<Embedding> out) throws Exception {
    if (predicates.evaluate(EmbeddingFactory.fromEdge(edge, filterPropertyKeys), filterMetaData)) {
      out.collect(EmbeddingFactory.fromEdge(edge, projectionPropertyKeys));
    }
  }
}
