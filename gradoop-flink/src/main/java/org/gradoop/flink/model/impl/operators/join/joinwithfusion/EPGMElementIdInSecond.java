package org.gradoop.flink.model.impl.operators.join.joinwithfusion;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Created by vasistas on 15/02/17.
 */
public class EPGMElementIdInSecond<K extends EPGMElement> implements
  KeySelector<Tuple2<GradoopId, K>, GradoopId> {
  @Override
  public GradoopId getKey(Tuple2<GradoopId, K> value) throws Exception {
    return value.f1.getId();
  }
}
