
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.algorithms.fsm.transactional.CategoryCharacteristicSubgraphs;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.CCSGraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;

import java.util.Map;

/**
 * graphTransaction => categorizedGraph
 */
public class ToCCSGraph extends ToFSMGraph
  implements MapFunction<GraphTransaction, CCSGraph> {

  @Override
  public CCSGraph map(GraphTransaction graph) throws Exception {

    String category = graph.getGraphHead().getPropertyValue(
      CategoryCharacteristicSubgraphs.CATEGORY_KEY).toString();

    Map<GradoopId, Integer> vertexIdMap =
      Maps.newHashMapWithExpectedSize(graph.getVertices().size());

    Map<Integer, String> fsmVertices = transformVertices(graph, vertexIdMap);

    Map<Integer, FSMEdge> fsmEdges = transformEdges(graph, vertexIdMap);

    return new CCSGraph(
      category, graph.getGraphHead().getId(), fsmVertices, fsmEdges);
  }
}
