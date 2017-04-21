package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by vasistas on 20/04/17.
 */
@FunctionAnnotation.ForwardedFieldsFirst("id -> id; label -> label; properties -> properties")
@FunctionAnnotation.ForwardedFieldsSecond("f1 -> graphIds")
public class AssociateVertexToGraphIds implements
  JoinFunction<Vertex, Tuple2<GradoopId, GradoopIdList>, Vertex> {
  @Override
  public Vertex join(Vertex first, Tuple2<GradoopId, GradoopIdList> second) throws Exception {
    first.setGraphIds(second.f1);
    return first;
  }
}
