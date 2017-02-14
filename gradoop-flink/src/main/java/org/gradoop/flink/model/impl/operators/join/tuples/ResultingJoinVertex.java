package org.gradoop.flink.model.impl.operators.join.tuples;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.join.operators.OptSerializable;
import org.gradoop.flink.model.impl.operators.join.operators.OptSerializableGradoopId;

import java.io.Serializable;

/**
 * Created by Giacomo Bergami on 30/01/17.
 */
public class ResultingJoinVertex extends
  Tuple3<OptSerializableGradoopId, OptSerializableGradoopId, Vertex> implements Serializable {
  public ResultingJoinVertex(OptSerializableGradoopId empty, OptSerializableGradoopId value,
    Vertex second) {
    super(empty,value,second);
  }
  public ResultingJoinVertex() {}
}
