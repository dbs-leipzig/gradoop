package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.join.common.tuples.DisambiguationTupleWithVertexId;

/**
 * Created by Giacomo Bergami on 16/02/17.
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class VertexNotInSubgraph implements MapFunction<Vertex, DisambiguationTupleWithVertexId> {

  /**
   * Reusage element
   */
  private final DisambiguationTupleWithVertexId reusable;

  public VertexNotInSubgraph() {
    reusable = new DisambiguationTupleWithVertexId();
    reusable.f1 = false;
    reusable.f2 = GradoopId.NULL_VALUE;
  }

  @Override
  public DisambiguationTupleWithVertexId map(Vertex value) throws Exception {
    reusable.f0 = value;
    return reusable;
  }
}
