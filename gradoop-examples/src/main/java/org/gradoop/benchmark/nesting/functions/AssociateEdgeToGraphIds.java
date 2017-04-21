package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Created by vasistas on 20/04/17.
 */
@FunctionAnnotation.ForwardedFieldsFirst("id -> id; label -> label; properties -> properties; " +
  "sourceId -> sourceId; targetId -> targetId")
@FunctionAnnotation.ForwardedFieldsSecond("f1 -> graphIds")
public class AssociateEdgeToGraphIds implements
  JoinFunction<Edge, Tuple2<GradoopId, GradoopIdList>, Edge> {
  @Override
  public Edge join(Edge first, Tuple2<GradoopId, GradoopIdList> second) throws Exception {
    first.setGraphIds(second.f1);
    return first;
  }
}
