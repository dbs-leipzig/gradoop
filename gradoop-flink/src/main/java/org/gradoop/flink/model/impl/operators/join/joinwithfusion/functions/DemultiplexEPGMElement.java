package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by vasistas on 15/02/17.
 */
public class DemultiplexEPGMElement<K extends GraphElement> implements FlatMapFunction<K,
  Tuple2<GradoopId,
  K>> {
  private final Tuple2<GradoopId,K> REUSABLE = new Tuple2<GradoopId, K>();

  @Override
  public void flatMap(K value, Collector<Tuple2<GradoopId, K>> out) throws Exception {
    for (GradoopId id : value.getGraphIds()) {
      REUSABLE.f0 = id;
      REUSABLE.f1 = value;
      out.collect(REUSABLE);
    }
  }
}
