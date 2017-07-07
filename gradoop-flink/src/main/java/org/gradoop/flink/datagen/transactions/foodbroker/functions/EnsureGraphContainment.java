package org.gradoop.flink.datagen.transactions.foodbroker.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.representation.transactional.GraphTransaction;

/**
 * Created by peet on 07.07.17.
 */
public class EnsureGraphContainment implements MapFunction<GraphTransaction, GraphTransaction> {

  @Override
  public GraphTransaction map(GraphTransaction graph) throws Exception {
    GradoopIdList graphIds = GradoopIdList.fromExisting(graph.getGraphHead().getId());

    for (Vertex vertex : graph.getVertices()) {
      vertex.setGraphIds(graphIds);
    }
    return graph;
  }
}
