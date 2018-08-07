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

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Distributes the new vertex value
 */
public class LPMessageFunction
  extends ScatterFunction<GradoopId, PropertyValue, PropertyValue, NullValue> {

  @Override
  public void sendMessages(Vertex<GradoopId, PropertyValue> vertex) throws
    Exception {
    sendMessageToAllNeighbors(vertex.getValue());
  }
}
