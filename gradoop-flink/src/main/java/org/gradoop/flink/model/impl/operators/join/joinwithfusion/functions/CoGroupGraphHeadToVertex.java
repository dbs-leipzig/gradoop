package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by vasistas on 17/02/17.
 */
public class CoGroupGraphHeadToVertex implements
  CoGroupFunction<Tuple2<Vertex, GradoopId>, GraphHead, Tuple2<Vertex, GradoopId>> {

  private final Tuple2<Vertex, GradoopId> reusable;

  public CoGroupGraphHeadToVertex() {
    reusable = new Tuple2<>();
    reusable.f0 = new Vertex();
  }

  @Override
  public void coGroup(Iterable<Tuple2<Vertex, GradoopId>> first, Iterable<GraphHead> second,
    Collector<Tuple2<Vertex, GradoopId>> out) throws Exception {
    for (Tuple2<Vertex, GradoopId> x : first) {
      for (GraphHead hid : second) {
        reusable.f0.setId(GradoopId.get());
        reusable.f0.setLabel(hid.getLabel());
        reusable.f0.setProperties(hid.getProperties());
        reusable.f1 = hid.getId();
        out.collect(reusable);
        break;
      }
      break;
    }
  }
}
