package org.gradoop.flink.model.impl.operators.fusion.reduce.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Collection;

/**
 * Created by Giacomo Bergami on 17/02/17.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")
@FunctionAnnotation.ReadFieldsSecond("f0")
public class CoGroupAssociateOldVerticesWithNewIds implements
  CoGroupFunction<Tuple2<Vertex, GradoopId>, Tuple2<Vertex, GradoopId>, Tuple2<Vertex,GradoopId>> {

  private final Tuple2<Vertex,GradoopId> reusable;
  private final Collection<GradoopId> reusableList;

  public CoGroupAssociateOldVerticesWithNewIds() {
    reusable = new Tuple2<>();
    reusableList = Lists.newArrayList();
  }

  @Override
  public void coGroup(Iterable<Tuple2<Vertex, GradoopId>> first,
    Iterable<Tuple2<Vertex, GradoopId>> second, Collector<Tuple2<Vertex, GradoopId>> out) throws
    Exception {
    reusableList.clear();
    second.forEach(x -> reusableList.add(x.f0.getId()));
    for (Tuple2<Vertex,GradoopId> x : first) {
      for (GradoopId y : reusableList) {
        reusable.f0 = x.f0;
        reusable.f1 = y;
        out.collect(reusable);
      }
    }
  }
}
