package org.gradoop.model.impl.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

public class SetBtg<E extends EPGMGraphElement>
  implements JoinFunction<E, Tuple2<GradoopId, GradoopId>, E> {

  @Override
  public E join(E element, Tuple2<GradoopId, GradoopId> mapping) {
    element.setGraphIds(GradoopIdSet.fromExisting(mapping.f1));
    return element;
  }
}
