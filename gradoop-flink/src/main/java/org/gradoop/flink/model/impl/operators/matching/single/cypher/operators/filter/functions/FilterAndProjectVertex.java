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
  private final List<String> propertyKeys;
  /**
   * Meta data describing the vertex embedding used for filtering
   */
  private final EmbeddingMetaData metaData;

  /**
   * New vertex filter function
   *
   * @param predicates predicates used for filtering
   * @param metaData meta data describing the output embedding
   */
  public FilterAndProjectVertex(CNF predicates, EmbeddingMetaData metaData) {
    this.predicates = predicates;
    this.metaData = metaData;
    this.propertyKeys = metaData.getPropertyKeys(metaData.getVariables().get(0));
  }

  @Override
  public void flatMap(Vertex vertex, Collector<Embedding> out) throws Exception {
    Embedding embedding = EmbeddingFactory.fromVertex(vertex, propertyKeys);

    if (predicates.evaluate(embedding, metaData)) {
      out.collect(embedding);
    }
  }
}
