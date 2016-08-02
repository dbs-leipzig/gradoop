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

package org.gradoop.model.impl.algorithms.labelpropagation.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.api.entities.EPGMVertex;

/**
 * Updates the vertex on the left side with the property value on the right side
 *
 * @param <V> EPGM vertex type
 */
@FunctionAnnotation.ForwardedFieldsSecond("id;label;graphIds")
@FunctionAnnotation.ReadFieldsFirst("f1")
public class LPVertexJoin<V extends EPGMVertex>
  implements JoinFunction<org.apache.flink.graph.Vertex, V, V> {

  /**
   * Property key to access the value which will be propagated
   */
  private final String propertyKey;

  /**
   * Constructor
   *
   * @param propertyKey property key to access the propagation value
   */
  public LPVertexJoin(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public V join(org.apache.flink.graph.Vertex gellyVertex, V epgmVertex)
      throws Exception {
    epgmVertex.setProperty(propertyKey, gellyVertex.getValue());
    return epgmVertex;
  }
}
