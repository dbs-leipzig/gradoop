package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by vasistas on 15/02/17.
 */
public class MergeToBeFusedVertices implements
  CoGroupFunction<Tuple2<GradoopId,Vertex>, GraphHead, Tuple2<GradoopId,Vertex>> {

  final Vertex v;

  public MergeToBeFusedVertices(GradoopId gid) {
    this.v = new Vertex();
    this.v.addGraphId(gid);
  }

  @Override
  public void coGroup(Iterable<Tuple2<GradoopId, Vertex>> first, Iterable<GraphHead> second,
    Collector<Tuple2<GradoopId, Vertex>> out) throws Exception {
    for (GraphHead gh : second) {
      v.setProperties(gh.getProperties());
      v.setLabel(gh.getLabel());
      out.collect(new Tuple2<>(gh.getId(),v));
      break;
    }
  }
}
