
package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.representation.transactional.GraphTransaction;

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
