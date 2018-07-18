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
package org.gradoop.flink.algorithms.gelly.labelpropagation.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Updates the vertex on the left side with the property value on the right side
 */
@FunctionAnnotation.ForwardedFieldsSecond("id;label;graphIds")
@FunctionAnnotation.ReadFieldsFirst("f1")
public class LPVertexJoin implements JoinFunction
  <org.apache.flink.graph.Vertex<GradoopId, PropertyValue>, Vertex, Vertex> {

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
  public Vertex join(
    org.apache.flink.graph.Vertex<GradoopId, PropertyValue> gellyVertex,
    Vertex epgmVertex) throws Exception {
    epgmVertex.setProperty(propertyKey, gellyVertex.getValue());
    return epgmVertex;
  }
}
