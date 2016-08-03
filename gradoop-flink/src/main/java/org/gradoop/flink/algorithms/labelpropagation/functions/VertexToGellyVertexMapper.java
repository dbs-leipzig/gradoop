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

package org.gradoop.flink.algorithms.labelpropagation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Maps EPGM vertex to a Gelly vertex consisting of the EPGM identifier and the
 * label propagation value.
 */
@FunctionAnnotation.ForwardedFields("id->f0")
@FunctionAnnotation.ReadFields("properties")
public class VertexToGellyVertexMapper implements
  MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, PropertyValue>> {
  /**
   * Property key to access the label value which will be propagated
   */
  private final String propertyKey;

  /**
   * Reduce object instantiations
   */
  private final org.apache.flink.graph.Vertex<GradoopId, PropertyValue>
  reuseVertex;

  /**
   * Constructor
   *
   * @param propertyKey property key for get property value
   */
  public VertexToGellyVertexMapper(String propertyKey) {
    this.propertyKey = propertyKey;
    this.reuseVertex = new org.apache.flink.graph.Vertex<>();
  }

  @Override
  public org.apache.flink.graph.Vertex<GradoopId, PropertyValue> map(
    Vertex epgmVertex) throws Exception {
    reuseVertex.setId(epgmVertex.getId());
    reuseVertex.setValue(epgmVertex.getPropertyValue(propertyKey));
    return reuseVertex;
  }
}
