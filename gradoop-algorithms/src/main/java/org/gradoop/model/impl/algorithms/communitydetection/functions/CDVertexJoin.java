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

package org.gradoop.model.impl.algorithms.communitydetection.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Updates the vertex on the left side with the property value on the right side
 *
 * @param <V> EPGM vertex type
 */
public class CDVertexJoin<V extends EPGMVertex>
  implements JoinFunction<Vertex<GradoopId, Long>, V, V> {

  /**
   * Property key to access the community value
   */
  private String propertyKey;


  /**
   * Constructor
   *
   * @param propertyKey property key to access the community value
   */
  public CDVertexJoin(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public V join(Vertex<GradoopId, Long> gellyVertex, V epgmVertex)
      throws Exception {
    epgmVertex.setProperty(propertyKey, gellyVertex.getValue());
    return epgmVertex;
  }
}
