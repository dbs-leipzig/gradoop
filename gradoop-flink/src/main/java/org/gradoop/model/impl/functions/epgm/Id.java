package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopId;

public class Id<EL extends EPGMElement>
  implements MapFunction<EL, GradoopId>, KeySelector<EL, GradoopId> {

  @Override
  public GradoopId map(EL element) throws Exception {
    return element.getId();
  }

  @Override
  public GradoopId getKey(EL element) throws Exception {
    return element.getId();
  }
}