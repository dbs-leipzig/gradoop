
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Maps;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Map;

/**
 * Superclass of map functions mapping a graph transaction to a FSM-fitted
 * graph format.
 */
public abstract class ToFSMGraph {

  /**
   * Transforms a graph's vertices.
   *
   * @param graph graph
   * @param vertexIdMap mapping of Gradoop ids to integers
   * @return vertices represented by a integer-label map
   */
  protected Map<Integer, String> transformVertices(GraphTransaction graph,
    Map<GradoopId, Integer> vertexIdMap) {

    Map<Integer, String> fsmVertices =
      Maps.newHashMapWithExpectedSize(graph.getVertices().size());

    int vertexId = 0;
    for (Vertex vertex : graph.getVertices()) {
      vertexIdMap.put(vertex.getId(), vertexId);
      fsmVertices.put(vertexId, vertex.getLabel());
      vertexId++;
    }
    return fsmVertices;
  }

  /**
   * Transforms a graph's edges.
   *
   * @param graph graph
   * @param vertexIdMap mapping of Gradoop ids to integers
   * @return id-edge map
   */
  protected Map<Integer, FSMEdge> transformEdges(GraphTransaction graph,
    Map<GradoopId, Integer> vertexIdMap) {
    Map<Integer, FSMEdge> fsmEdges =
      Maps.newHashMapWithExpectedSize(graph.getEdges().size());

    int edgeId = 0;
    for (Edge edge : graph.getEdges()) {

      int sourceId = vertexIdMap.get(edge.getSourceId());
      int targetId = vertexIdMap.get(edge.getTargetId());

      fsmEdges.put(edgeId, new FSMEdge(sourceId, edge.getLabel(), targetId));

      edgeId++;
    }
    return fsmEdges;
  }
}
