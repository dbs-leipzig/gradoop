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

package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * generates a string representation fo an outgoing adjacency list
 */
public class UndirectedAdjacencyList implements
  GroupReduceFunction<EdgeString, VertexString> {

  @Override
  public void reduce(Iterable<EdgeString> outgoingEdgeLabels,
    Collector<VertexString> collector) throws Exception {

    Boolean first = true;
    GradoopId vertexId = null;
    GradoopId graphId = null;

    List<String> adjacencyListEntries = new ArrayList<>();

    for (EdgeString edgeString : outgoingEdgeLabels) {
      if (first) {
        graphId = edgeString.getGraphId();
        vertexId = edgeString.getSourceId();
        first = false;
      }

      adjacencyListEntries.add("\n  -" + edgeString.getEdgeLabel() + "-" +
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
