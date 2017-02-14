package org.gradoop.flink.model.impl.operators.join.blocks;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.tuple.Project2To1;
import org.gradoop.flink.model.impl.operators.join.operators.OptSerializable;
import org.gradoop.flink.model.impl.operators.join.operators.OptSerializableGradoopId;

import java.io.Serializable;

/**
 * Defines a KeySelector from the second projection of a tuple
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
public class KeySelectorFromRightProjection implements KeySelector<Tuple2<Vertex,
  OptSerializableGradoopId>,Integer>,
  Serializable {

  public KeySelectorFromRightProjection() {
  }

  @Override
  public Integer getKey(Tuple2<Vertex,OptSerializableGradoopId> value) throws Exception {
    return value.f1.hashCode();
  }
}
