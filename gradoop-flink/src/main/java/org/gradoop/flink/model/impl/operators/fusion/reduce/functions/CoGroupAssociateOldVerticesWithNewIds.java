package org.gradoop.flink.model.impl.operators.fusion.reduce.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by Giacomo Bergami on 17/02/17.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")
@FunctionAnnotation.ReadFieldsSecond("f0")
public class CoGroupAssociateOldVerticesWithNewIds implements
  CoGroupFunction<Tuple2<Vertex, GradoopId>, Tuple2<Vertex, GradoopId>, Tuple2<Vertex,GradoopId>> {

  private final Tuple2<Vertex,GradoopId> reusable;

  public CoGroupAssociateOldVerticesWithNewIds() {
    reusable = new Tuple2<>();
  }

  @Override
  public void coGroup(Iterable<Tuple2<Vertex, GradoopId>> first,
    Iterable<Tuple2<Vertex, GradoopId>> second, Collector<Tuple2<Vertex, GradoopId>> out) throws
    Exception {
    for (Tuple2<Vertex,GradoopId> x : first) {
      for (Tuple2<Vertex,GradoopId> y : second) {
        reusable.f0 = x.f0;
        reusable.f1 = y.f0.getId();
        out.collect(reusable);
      }
    }
  }
}
