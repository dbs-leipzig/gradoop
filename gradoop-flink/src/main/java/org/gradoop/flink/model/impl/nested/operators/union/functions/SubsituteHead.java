package org.gradoop.flink.model.impl.nested.operators.union.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Changes the head value with another one. It is used in the union function
 */
@FunctionAnnotation.ForwardedFields("f1 -> f1")
public class SubsituteHead implements FlatMapFunction<Tuple2<GradoopId, GradoopId>,
  Tuple2<GradoopId, GradoopId>> {

  /**
   * reusable element, to be returned
   */
  private final Tuple2<GradoopId, GradoopId> reusable;

  /**
   * Changes the head of the IdGraphDatabase information mapping it
   * @param id  New graph id
   */
  public SubsituteHead(GradoopId id) {
    reusable = new Tuple2<>();
    reusable.f0 = id;
  }

  @Override
  public void flatMap(Tuple2<GradoopId, GradoopId> value, Collector<Tuple2<GradoopId, GradoopId>>
    out)
    throws Exception {
    reusable.f1 = value.f1;
    out.collect(reusable);
  }

}
