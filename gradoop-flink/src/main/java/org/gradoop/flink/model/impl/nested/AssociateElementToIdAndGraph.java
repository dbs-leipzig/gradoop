package org.gradoop.flink.model.impl.nested;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by vasistas on 10/03/17.
 */
public class AssociateElementToIdAndGraph<X extends GraphElement> implements
  FlatMapFunction<X, Tuple2<GradoopId,GradoopId>> {

  private final Tuple2<GradoopId, GradoopId> reusable;

  public AssociateElementToIdAndGraph() {
    reusable = new Tuple2<>();
  }

  @Override
  public void flatMap(X value, Collector<Tuple2<GradoopId, GradoopId>> out) throws Exception {
    reusable.f1 = value.getId();
    for (GradoopId gid : value.getGraphIds()) {
      reusable.f0 = gid;
      out.collect(reusable);
    }
  }
}
