package org.gradoop.flink.model.impl.nested;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Created by vasistas on 10/03/17.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f1->*")
public class GraphHeadMatch implements JoinFunction<Tuple2<GradoopId,GradoopId>,GradoopId,GradoopId> {

  @Override
  public GradoopId join(Tuple2<GradoopId, GradoopId> first, GradoopId second) throws Exception {
    return first.f1;
  }
}
