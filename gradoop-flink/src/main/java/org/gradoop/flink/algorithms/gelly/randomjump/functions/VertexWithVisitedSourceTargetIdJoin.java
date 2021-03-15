/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Joins an Gradoop vertex with the source- resp. target-id of a visited edge. Sets the vertex visited
 * property to {@code true}, if there is a join-partner.
 *
 * @param <V> Gradoop Vertex type
 */
@FunctionAnnotation.ForwardedFieldsFirst("id;label;graphIds")
public class VertexWithVisitedSourceTargetIdJoin<V extends Vertex> implements JoinFunction<V, GradoopId, V> {

  /**
   * Key for the boolean property of the edge.
   */
  private final String propertyKey;

  /**
   * Creates an instance of VertexWithVisitedSourceTargetIdJoin with a given property key.
   *
   * @param propertyKey propertyKey Key for the boolean property value.
   */
  public VertexWithVisitedSourceTargetIdJoin(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public V join(V vertex, GradoopId visitedId) throws Exception {
    if (visitedId != null) {
      vertex.setProperty(propertyKey, true);
    }
    return vertex;
  }
}
