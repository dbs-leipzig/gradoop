package org.gradoop.flink.model.impl.operators.join.blocks;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.io.Serializable;

/**
 * Created by Giacomo Bergami on 01/02/17.
 */
public class KeySelectorFromTupleProjetionWithGradoopId implements KeySelector<Tuple2<GradoopId,
  Vertex>,GradoopId>, Serializable {
  @Override
  public GradoopId getKey(Tuple2<GradoopId, Vertex> value) throws Exception {
    return value.f0;
  }
}
