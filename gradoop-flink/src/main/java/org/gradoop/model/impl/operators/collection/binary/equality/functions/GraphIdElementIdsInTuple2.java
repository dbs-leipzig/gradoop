package org.gradoop.model.impl.operators.collection.binary.equality.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;

/**
 * Created by peet on 19.11.15.
 */
public class GraphIdElementIdsInTuple2 implements GroupReduceFunction
  <Tuple2<GradoopId,GradoopId>,Tuple2<GradoopId,GradoopIds>>{

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, GradoopId>> iterable,
    Collector<Tuple2<GradoopId, GradoopIds>> collector) throws Exception {

    boolean first = true;

    GradoopId graphId = null;
    GradoopIds elementIds = null;

    for(Tuple2<GradoopId, GradoopId> graphIdElementId : iterable) {
      if(first) {
        first = false;
        graphId = graphIdElementId.f0;
        elementIds = GradoopIds.fromExisting(graphIdElementId.f1);
      } else {
        elementIds.add(graphIdElementId.f1);
      }
    }

    collector.collect(new Tuple2<>(graphId, elementIds));
  }
}
