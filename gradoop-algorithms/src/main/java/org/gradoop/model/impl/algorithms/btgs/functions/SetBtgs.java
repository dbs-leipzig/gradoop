package org.gradoop.model.impl.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

public class SetBtgs<EL extends EPGMGraphElement>
  implements JoinFunction<EL, Tuple2<GradoopId, GradoopIdSet>, EL> {

  @Override
  public EL join(EL element, Tuple2<GradoopId, GradoopIdSet> mapping) throws
    Exception {
    element.setGraphIds(mapping.f1);
    return element;
  }
}
