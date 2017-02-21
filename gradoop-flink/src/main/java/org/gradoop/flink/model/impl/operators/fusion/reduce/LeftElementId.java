package org.gradoop.flink.model.impl.operators.fusion.reduce;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Left projection with id obtained.
 *
 * Created by Giacomo Bergami on 21/02/17.
 */
public class LeftElementId<K extends EPGMElement> implements KeySelector<Tuple2<K, GradoopId>,
  GradoopId> {
  @Override
  public GradoopId getKey(Tuple2<K, GradoopId> value) throws Exception {
    return value.f0.getId();
  }
}
