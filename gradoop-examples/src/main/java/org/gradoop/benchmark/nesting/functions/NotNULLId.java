package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Created by vasistas on 10/04/17.
 */
public class NotNULLId implements FilterFunction<GradoopId> {
  @Override
  public boolean filter(GradoopId value) throws Exception {
    return !value.equals(GradoopId.NULL_VALUE);
  }
}
