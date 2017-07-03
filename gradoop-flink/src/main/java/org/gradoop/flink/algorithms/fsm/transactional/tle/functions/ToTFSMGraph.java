
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.TFSMGraph;

import java.util.Map;

/**
 * graphTransaction => fsmGraph
 */
public class ToTFSMGraph extends ToFSMGraph
  implements MapFunction<GraphTransaction, TFSMGraph> {

  @Override
  public TFSMGraph map(GraphTransaction graph) throws Exception {

    Map<GradoopId, Integer> vertexIdMap =
      Maps.newHashMapWithExpectedSize(graph.getVertices().size());

    Map<Integer, String> fsmVertices = transformVertices(graph, vertexIdMap);

    Map<Integer, FSMEdge> fsmEdges = transformEdges(graph, vertexIdMap);

    return new TFSMGraph(graph.getGraphHead().getId(), fsmVertices, fsmEdges);
  }
}
