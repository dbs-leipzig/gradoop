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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Maps EPGM vertex to a Gelly vertex consisting of the EPGM identifier and the
 * label propagation value.
 *
 * @param <V> EPGM vertex type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
@FunctionAnnotation.ReadFields("properties")
public class VertexToGellyVertexMapper<V extends EPGMVertex>
  implements MapFunction<V, Vertex<GradoopId, PropertyValue>> {
  /**
   * Property key to access the label value which will be propagated
   */
  private final String propertyKey;

  /**
   * Reduce object instantiations
   */
  private final Vertex<GradoopId, PropertyValue> reuseVertex;

  /**
   * Constructor
   *
   * @param propertyKey property key for get property value
   */
  public VertexToGellyVertexMapper(String propertyKey) {
    this.propertyKey = propertyKey;
    this.reuseVertex = new Vertex<>();
  }

  @Override
  public Vertex<GradoopId, PropertyValue> map(V epgmVertex) throws Exception {
    reuseVertex.setId(epgmVertex.getId());
    reuseVertex.setValue(epgmVertex.getPropertyValue(propertyKey));
    return reuseVertex;
  }
}
