
package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Returns a tuple which contains the source id and target id of an edge and a tuple which contains
 * the target id and the source id of the same edge.
 */
public class ShuffledVertexIdsFromEdge
  implements FlatMapFunction<Edge, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reuse tuple to avoid instantiations.
   */
  private Tuple2<GradoopId, GradoopId> reuseTuple;

  /**
   * Constructor which instantiates the reuse tuple.
   */
  public ShuffledVertexIdsFromEdge() {
    reuseTuple = new Tuple2<GradoopId, GradoopId>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(Edge edge, Collector<Tuple2<GradoopId, GradoopId>> collector)
    throws Exception {
    reuseTuple.setFields(edge.getSourceId(), edge.getTargetId());
    collector.collect(reuseTuple);
    reuseTuple.setFields(edge.getTargetId(), edge.getSourceId());
    collector.collect(reuseTuple);
  }
}
