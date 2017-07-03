
package org.gradoop.flink.datagen.transactions.foodbroker.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Creates tuples containing the target id of an edge together with the edges graph id.
 */
public class TargetGraphIdPair implements MapFunction<Edge, Tuple2<GradoopId, GradoopId>> {

  @Override
  public Tuple2<GradoopId, GradoopId> map(Edge edge) throws Exception {
    return new Tuple2<>(edge.getTargetId(), edge.getGraphIds().iterator().next());
  }
}
