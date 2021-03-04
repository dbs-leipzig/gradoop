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
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Joins an Gradoop edge with a GradoopId of an visited edge from the VCI run. Assigns a boolean
 * property to the Gradoop edge, determining if this edge was visited. The property is set to
 * {@code true} if the Gradoop edge has a join partner, set to {@code false} otherwise.
 *
 * @param <E> Gradoop Edge type
 */
@FunctionAnnotation.ForwardedFieldsFirst("id;sourceId;targetId;label;graphIds")
public class EdgeWithGellyEdgeIdJoin<E extends Edge> implements JoinFunction<E, GradoopId, E> {

  /**
   * Key for the boolean property of the edge.
   */
  private final String propertyKey;

  /**
   * Creates an instance of EdgeWithGellyEdgeIdJoin with a given key for the boolean property value.
   *
   * @param propertyKey propertyKey Key for the boolean property value.
   */
  public EdgeWithGellyEdgeIdJoin(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public E join(E edge, GradoopId visitedEdgeId) throws Exception {
    boolean visited = false;
    if (visitedEdgeId != null) {
      visited = true;
    }
    edge.setProperty(propertyKey, visited);
    return edge;
  }
}
