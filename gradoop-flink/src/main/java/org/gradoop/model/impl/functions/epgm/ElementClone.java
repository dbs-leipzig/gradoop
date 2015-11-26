package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopId;

public class ElementClone<EL extends EPGMElement>
  implements MapFunction<EL, EL>{

  @Override
  public EL map(EL el) throws Exception {
    el.setId(GradoopId.get());
    return el;
  }
}
