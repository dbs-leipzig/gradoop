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
package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

/**
 * drops properties and graph containment of vertices and edges.
 */
public class DropPropertiesAndGraphContainment
  implements MapFunction<GraphTransaction, GraphTransaction> {

  @Override
  public GraphTransaction map(GraphTransaction transaction) throws Exception {

    for (Vertex vertex : transaction.getVertices()) {
      vertex.setProperties(null);
      vertex.setGraphIds(null);
    }

    for (Edge edge : transaction.getEdges()) {
      edge.setProperties(null);
      edge.setGraphIds(null);
    }

    return transaction;
  }
}
