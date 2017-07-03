
package org.gradoop.flink.datagen.transactions.foodbroker.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Updates the graph id list of a vertex to the one of the tuple's second element. The first
 * element is the gradoop id of the vertex.
 */
public class UpdateGraphIds
  implements JoinFunction<Tuple2<GradoopId, GradoopIdList>, Vertex, Vertex> {

  @Override
  public Vertex join(Tuple2<GradoopId, GradoopIdList> pair, Vertex vertex) throws Exception {
    vertex.setGraphIds(pair.f1);
    return vertex;
  }
}
