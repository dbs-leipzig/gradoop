
package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Returns a tuple which contains the source id and the edge and a tuple which contains the
 * target id and same edge.
 */
public class VertexIdsWithEdge implements FlatMapFunction<Edge, Tuple2<GradoopId, Edge>> {

  /**
   * Reuse tuple to avoid instantiations.
   */
  private Tuple2<GradoopId, Edge> reuseTuple;

  /**
   * Constructor which instantiates the reuse tuple.
   */
  public VertexIdsWithEdge() {
    reuseTuple = new Tuple2<GradoopId, Edge>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(Edge edge, Collector<Tuple2<GradoopId, Edge>> collector) throws Exception {
    reuseTuple.setFields(edge.getSourceId(), edge);
    collector.collect(reuseTuple);
    reuseTuple.setFields(edge.getTargetId(), edge);
    collector.collect(reuseTuple);
  }
}
