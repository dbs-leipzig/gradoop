
package org.gradoop.flink.algorithms.fsm.dimspan.functions.conversion;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Map;

/**
 * Gradoop Graph Transaction => lightweight labeled graph
 */
public class EPGMGraphTransactionToLabeledGraph implements
  MapFunction<GraphTransaction, LabeledGraphStringString> {

  @Override
  public LabeledGraphStringString map(GraphTransaction transaction) throws Exception {

    LabeledGraphStringString outGraph = LabeledGraphStringString.getEmptyOne();

    Map<String, Integer> labelMap = Maps.newHashMap();
    Map<GradoopId, Integer> vertexIdMap = Maps.newHashMap();

    for (Vertex vertex : transaction.getVertices()) {
      vertexIdMap.put(vertex.getId(), outGraph.addVertex(vertex.getLabel()));
    }

    labelMap.clear();

    for (Edge edge : transaction.getEdges()) {
      int sourceId = vertexIdMap.get(edge.getSourceId());
      int targetId = vertexIdMap.get(edge.getTargetId());
      outGraph.addEdge(sourceId, edge.getLabel(), targetId);
    }

    return outGraph;
  }
}
