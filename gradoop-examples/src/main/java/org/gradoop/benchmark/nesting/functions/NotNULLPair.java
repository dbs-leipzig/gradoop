package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Created by vasistas on 10/04/17.
 */
public class NotNULLPair implements FilterFunction<Tuple2<GradoopId, GradoopId>> {
  @Override
  public boolean filter(Tuple2<GradoopId, GradoopId> value) throws Exception {
    return value != null &&
            !value.f0.equals(GradoopId.NULL_VALUE) &&
            !value.f1.equals(GradoopId.NULL_VALUE);
  }
}
