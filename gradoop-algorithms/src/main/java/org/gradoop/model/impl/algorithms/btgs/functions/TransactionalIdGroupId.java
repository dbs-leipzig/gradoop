package org.gradoop.model.impl.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 01.02.16.
 */
public class TransactionalIdGroupId<V extends EPGMVertex>
  implements MapFunction<V, Tuple2<GradoopId, GradoopId>> {
  @Override
  public Tuple2<GradoopId, GradoopId> map(V v) throws Exception {

    GradoopId id = v.getId();

    return new Tuple2<>(id, id);
  }
}
