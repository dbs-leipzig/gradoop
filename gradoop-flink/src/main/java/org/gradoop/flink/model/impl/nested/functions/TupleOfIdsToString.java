package org.gradoop.flink.model.impl.nested.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Created by vasistas on 10/03/17.
 */
public class TupleOfIdsToString implements KeySelector<Tuple2<GradoopId, GradoopId>, String> {
  @Override
  public String getKey(Tuple2<GradoopId, GradoopId> value) throws Exception {
    return value.toString();
  }
}
