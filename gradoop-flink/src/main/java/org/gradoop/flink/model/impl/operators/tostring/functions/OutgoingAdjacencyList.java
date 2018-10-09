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
package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * generates a string representation fo an outgoing adjacency list
 */
public class OutgoingAdjacencyList
  implements GroupReduceFunction<EdgeString, VertexString> {

  @Override
  public void reduce(Iterable<EdgeString> outgoingEdgeLabels,
    Collector<VertexString> collector) throws Exception {

    boolean first = true;
    GradoopId vertexId = null;
    GradoopId graphId = null;

    List<String> adjacencyListEntries = new ArrayList<>();

    for (EdgeString edgeString : outgoingEdgeLabels) {
      if (first) {
        graphId = edgeString.getGraphId();
        vertexId = edgeString.getSourceId();
        first = false;
      }

      adjacencyListEntries.add("\n  -" + edgeString.getEdgeLabel() + "->" +
        edgeString.getTargetLabel());
    }

    Collections.sort(adjacencyListEntries);

    collector.collect(new VertexString(
      graphId,
      vertexId,
      StringUtils.join(adjacencyListEntries, "")
    ));
  }
}
