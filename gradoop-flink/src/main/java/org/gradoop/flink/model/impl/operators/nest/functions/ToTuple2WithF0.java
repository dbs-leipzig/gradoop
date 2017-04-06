package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Creates an element of the stack from an head id
 */
@FunctionAnnotation.ForwardedFields("* -> f1")
public class ToTuple2WithF0 implements MapFunction<GradoopId, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reusable Element
   */
  private final Tuple2<GradoopId, GradoopId> reusable;

  /**
   * Default constructior
   * @param nestedGraphId   Id associated to the new graph in the EPGM model
   */
  public ToTuple2WithF0(GradoopId nestedGraphId) {
    reusable = new Tuple2<>();
    reusable.f0 = nestedGraphId;
  }

  @Override
  public Tuple2<GradoopId, GradoopId> map(GradoopId gradoopId) throws Exception {
    reusable.f1 = gradoopId;
    return reusable;
  }
}
