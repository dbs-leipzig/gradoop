package org.gradoop.flink.model.impl.nested.datastructures.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Associates each element to its belonging graph. This belonging
 * is not necessairly stored in the vertex, because all the
 * belongs-to-graph information is kept in the distributed
 * indexing structure, that is the IdGraphDatabase
 */
public class AssociateElementToIdAndGraph<X extends GraphElement> implements
  FlatMapFunction<X, Tuple2<GradoopId, GradoopId>> {

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
