package org.gradoop.benchmark.nesting;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Created by vasistas on 08/04/17.
 */
@FunctionAnnotation.ForwardedFields("f2.id -> *")
public class TripleWithGraphHeadToId
  implements MapFunction<Tuple3<String, Boolean, GraphHead>, GradoopId> {
  @Override
  public GradoopId map(Tuple3<String, Boolean, GraphHead> value) throws Exception {
    return value.f2.getId();
  }
}
