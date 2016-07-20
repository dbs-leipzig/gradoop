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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Maps EPGM vertex to a Gelly vertex consisting of the EPGM identifier and the
 * {@link Long} value.
 *
 * @param <V> EPGM vertex type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
@FunctionAnnotation.ReadFields("properties")
public class VertexToGellyVertexLongValueMapper<V extends EPGMVertex>
  implements MapFunction<V, Vertex<GradoopId, Long>> {
  /**
   * Property key to access the community value
   */
  private final String propertyKey;

  /**
   * Reduce object instantiations
   */
  private final Vertex<GradoopId, Long> reuseVertex;

  /**
   * Constructor
   *
   * @param propertyKey property key for get property value
   */
  public VertexToGellyVertexLongValueMapper(String propertyKey) {
    this.propertyKey = propertyKey;
    reuseVertex = new Vertex<>();
  }

  @Override
  public Vertex<GradoopId, Long> map(V v) throws Exception {
    reuseVertex.setId(v.getId());
    reuseVertex.setValue(v.getPropertyValue(propertyKey).getLong());
    return reuseVertex;
  }
}
