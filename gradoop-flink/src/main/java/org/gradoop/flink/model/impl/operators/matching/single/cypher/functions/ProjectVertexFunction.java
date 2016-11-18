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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.Projector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Projects a Vertex by a set of properties.
 * Vertex -> Embedding(GraphElementEmbedding(Vertex))
 */
public class ProjectVertexFunction extends RichMapFunction<Vertex, Embedding> {
  /**
   * Names of the properties that will be kept in the projection
   */
  private final Map<Integer, List<String>> propertyKeyMapping;

  /**
   * Creates a new vertex projection function
   * @param propertyKeys List of propertyKeys that will be kept in the projection
   */
  public ProjectVertexFunction(List<String> propertyKeys) {
    this.propertyKeyMapping = new HashMap<>();
    propertyKeyMapping.put(0, propertyKeys);
  }

  @Override
  public Embedding map(Vertex vertex) {
    if (propertyKeyMapping.get(0).isEmpty()) {
      return new Embedding(Lists.newArrayList(new IdEntry(vertex.getId())));
    } else {
      return Projector.project(Embedding.fromVertex(vertex), propertyKeyMapping);
    }
  }
}
