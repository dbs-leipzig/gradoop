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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingFactory;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

import java.util.List;

/**
 * Projects a Vertex by a set of properties.
 * Vertex -> Embedding(GraphElementEmbedding(Vertex))
 */
public class ProjectVertex extends RichMapFunction<Vertex, Embedding> {
  /**
   * Names of the properties that will be kept in the projection
   */
  private final List<String> propertyKeys;

  /**
   * Creates a new vertex projection function
   * @param propertyKeys List of propertyKeys that will be kept in the projection
   */
  public ProjectVertex(List<String> propertyKeys) {
    this.propertyKeys = propertyKeys;
  }

  @Override
  public Embedding map(Vertex vertex) {
    return EmbeddingFactory.fromVertex(vertex, propertyKeys);
  }
}
