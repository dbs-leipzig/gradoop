/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.algorithms.gelly.vertexdegrees.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees.Degrees;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Stores the in-degree, out-degree and the sum of both as a property in vertex
 */
public class DistinctVertexDegreesToAttribute implements JoinFunction<org.apache.flink.graph.Vertex<GradoopId, VertexDegrees.Degrees>, Vertex, Vertex> {

  /**
   * Property to store the sum of vertex degrees in.
   */
  private final String vertexDegreesPropery;
  /**
   * Property to store the in vertex degree in.
   */
  private final String vertexInDegreePropery;
  /**
   * Property to store the out vertex degree in.
   */
  private final String vertexOutDegreePropery;

  /**
   * Stores the in, out and sum of in and out degrees of a vertex.
   *
   * @param vertexDegreesPropery property key to store sum degree
   * @param vertexInDegreesPropery property key to store in degree
   * @param vertexOutDegreesPropery property key to store out degree
   */
  public DistinctVertexDegreesToAttribute(String vertexDegreesPropery,
      String vertexInDegreesPropery, String vertexOutDegreesPropery) {
    this.vertexDegreesPropery = vertexDegreesPropery;
    this.vertexInDegreePropery = vertexInDegreesPropery;
    this.vertexOutDegreePropery = vertexOutDegreesPropery;
  }

  @Override
  public Vertex join(org.apache.flink.graph.Vertex<GradoopId, Degrees> degree, Vertex vertex)
    throws Exception {
    vertex.setProperty(
        vertexDegreesPropery,
        PropertyValue.create(degree.getValue().getDegree().getValue()));
    vertex.setProperty(
        vertexInDegreePropery,
        PropertyValue.create(degree.getValue().getInDegree().getValue()));
    vertex.setProperty(
        vertexOutDegreePropery,
        PropertyValue.create(degree.getValue().getOutDegree().getValue()));
    return vertex;
  }
}
