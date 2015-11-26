package org.gradoop.model.impl.functions.isolation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopId;

public class Tuple1WithElementId<EL extends EPGMElement>
  implements MapFunction<EL, Tuple1<GradoopId>> {

  @Override
  public Tuple1<GradoopId> map(EL element) throws Exception {
    return new Tuple1<>(element.getId());
  }
}
