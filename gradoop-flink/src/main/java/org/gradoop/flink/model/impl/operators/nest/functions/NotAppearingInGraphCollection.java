package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

/**
 * Created by vasistas on 19/04/17.
 */
public class NotAppearingInGraphCollection implements FilterFunction<Hexaplet> {
  @Override
  public boolean filter(Hexaplet y) throws Exception {
    return !y.f4.equals(GradoopId.NULL_VALUE);
  }
}
