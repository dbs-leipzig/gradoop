package org.gradoop.model.impl.operators.matching.common.debug;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Maps the id of an EPGM element (vertex/edge) to an int property value.
 *
 * @param <EL> EPGM element type
 */
public class GradoopIdToDebugId<EL extends EPGMElement>
  implements MapFunction<EL, Tuple2<GradoopId, Integer>> {

  private final String propertyKey;

  public GradoopIdToDebugId(String propertyKey) {
    this.propertyKey = propertyKey;
  }
  @Override
  public Tuple2<GradoopId, Integer> map(EL el) throws Exception {
    return new Tuple2<>(el.getId(), el.getPropertyValue(propertyKey).getInt());
  }
}
