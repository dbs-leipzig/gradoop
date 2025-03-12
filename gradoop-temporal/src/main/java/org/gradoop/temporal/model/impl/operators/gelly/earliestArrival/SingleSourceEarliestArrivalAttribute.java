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
package org.gradoop.temporal.model.impl.operators.gelly.earliestArrival;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Stores the earliest arrival as a property in vertex.
 *
 * @param <V> Gradoop Vertex type
 */
public class SingleSourceEarliestArrivalAttribute<V extends Vertex>
  implements JoinFunction<org.apache.flink.graph.Vertex<GradoopId, Long>, V, V> {

  /**
   * Property to store the earliest arrival time in
   */
  private final String earliestArrivalProperty;

  /**
   * Stores the earliest arrival as a property.
   *
   * @param earliestArrivalProperty property key to store the earliest arrival in
   */
  public SingleSourceEarliestArrivalAttribute(String earliestArrivalProperty) {
    this.earliestArrivalProperty = earliestArrivalProperty;
  }

  @Override
  public V join(org.apache.flink.graph.Vertex<GradoopId, Long> gellyVertex, V vertex) {
    vertex.setProperty(earliestArrivalProperty, gellyVertex.getValue());
    return vertex;
  }
}
