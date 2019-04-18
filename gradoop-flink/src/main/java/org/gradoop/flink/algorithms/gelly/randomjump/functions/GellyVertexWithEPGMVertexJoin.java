/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.gelly.randomjump.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Joins a Gelly result vertex with an EPGM vertex. Assigning a boolean property value from the
 * Gelly vertex to the EPGM vertex, determining if this vertex was visited.
 */
@FunctionAnnotation.ReadFieldsFirst("f1")
@FunctionAnnotation.ForwardedFieldsSecond("id;label;graphIds")
public class GellyVertexWithEPGMVertexJoin implements
  JoinFunction<org.apache.flink.graph.Vertex<GradoopId, VCIVertexValue>, Vertex, Vertex> {

  /**
   * Key for the boolean property value to assign to the EPGM vertex.
   */
  private final String propertyKey;

  /**
   * Creates an instance of GellyVertexWithEPGMVertexJoin with a given key for
   * the boolean property value.
   *
   * @param propertyKey Key for the boolean property value.
   */
  public GellyVertexWithEPGMVertexJoin(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public Vertex join(org.apache.flink.graph.Vertex<GradoopId, VCIVertexValue> gellyVertex,
    Vertex epgmVertex) throws Exception {
    epgmVertex.setProperty(propertyKey, gellyVertex.getValue().f0);
    return epgmVertex;
  }
}
