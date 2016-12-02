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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.Projector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Projects an Edge by a set of properties.
 * Edge -> Embedding(IdEntry(SrcID), GraphElementEntry(Edge), IdEntry(TargetID))
 */
public class ProjectEdge extends RichMapFunction<Edge, Embedding> {
  /**
   * Names of the properties that will be kept in the projection
   */
  private final Map<Integer, List<String>> propertyKeyMapping;

  /**
   * Creates a new edge projection function
   * @param propertyKeys List of property names that will be kept in the projection
   */
  public ProjectEdge(List<String> propertyKeys) {
    this.propertyKeyMapping = new HashMap<>();
    propertyKeyMapping.put(1, propertyKeys);
  }

  @Override
  public Embedding map(Edge edge) {
    if (propertyKeyMapping.get(1).isEmpty()) {
      return new Embedding(Lists.newArrayList(
        new IdEntry(edge.getSourceId()),
        new IdEntry(edge.getId()),
        new IdEntry(edge.getTargetId())
      ));
    } else {
      return Projector.project(Embedding.fromEdge(edge), propertyKeyMapping);
    }
  }
}
