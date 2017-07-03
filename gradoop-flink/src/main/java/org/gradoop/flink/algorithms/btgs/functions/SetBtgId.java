
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Associates an edge with an business transaction graph.
 * @param <E> edge type
 */
public class SetBtgId<E extends EPGMGraphElement>
  implements JoinFunction<E, Tuple2<GradoopId, GradoopId>, E> {

  @Override
  public E join(E element, Tuple2<GradoopId, GradoopId> mapping) {
    element.setGraphIds(GradoopIdList.fromExisting(mapping.f1));
    return element;
  }
}
