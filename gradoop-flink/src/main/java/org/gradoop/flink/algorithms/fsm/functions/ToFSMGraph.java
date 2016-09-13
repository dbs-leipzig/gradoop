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
