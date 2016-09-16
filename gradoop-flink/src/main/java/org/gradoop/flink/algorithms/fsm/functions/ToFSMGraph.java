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

package org.gradoop.flink.algorithms.fsm.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.tuples.FSMGraph;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Map;

/**
 * graphTransaction => fsmGraph
 */
public class ToFSMGraph implements MapFunction<GraphTransaction, FSMGraph> {

  @Override
  public FSMGraph map(GraphTransaction graph) throws Exception {

    Map<GradoopId, Integer> vertexIdMap =
      Maps.newHashMapWithExpectedSize(graph.getVertices().size());

    Map<Integer, String> fsmVertices =
      Maps.newHashMapWithExpectedSize(graph.getVertices().size());

    Map<Integer, FSMEdge> fsmEdge =
      Maps.newHashMapWithExpectedSize(graph.getEdges().size());

    int vertexId = 0;
    for (Vertex vertex : graph.getVertices()) {
      vertexIdMap.put(vertex.getId(), vertexId);
      fsmVertices.put(vertexId, vertex.getLabel());
      vertexId++;
    }

    int edgeId = 0;
    for (Edge edge : graph.getEdges()) {

      int sourceId = vertexIdMap.get(edge.getSourceId());
      int targetId = vertexIdMap.get(edge.getTargetId());

      fsmEdge.put(edgeId, new FSMEdge(sourceId, edge.getLabel(), targetId));

      edgeId++;
    }

    return new FSMGraph(graph.getGraphHead().getId(), fsmVertices, fsmEdge);
  }
}
