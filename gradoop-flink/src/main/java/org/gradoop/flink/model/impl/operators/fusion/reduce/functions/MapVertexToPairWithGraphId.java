package org.gradoop.flink.model.impl.operators.fusion.reduce.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Demultiplexes a vertex by associating its graphId
 *
 * Created by Giacomo Bergami on 17/02/17.
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class MapVertexToPairWithGraphId implements FlatMapFunction<Vertex, Tuple2<Vertex,GradoopId>> {

  private final Tuple2<Vertex,GradoopId> reusableTuple;

  public MapVertexToPairWithGraphId() {
    reusableTuple = new Tuple2<>();
  }

  @Override
  public void flatMap(Vertex value, Collector<Tuple2<Vertex, GradoopId>> out) throws Exception {
    if (value!=null) {
      for (GradoopId id : value.getGraphIds()) {
        reusableTuple.f0 = value;
        reusableTuple.f1 = id;
        out.collect(reusableTuple);
      }
    }
  }
}
