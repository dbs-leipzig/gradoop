
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Associates a (master) vertex with business transaction graphs.
 * @param <V> vertex type
 */
public class SetBtgIds<V extends EPGMVertex>
  implements JoinFunction<V, Tuple2<GradoopId, GradoopIdList>, V> {

  @Override
  public V join(V element, Tuple2<GradoopId, GradoopIdList> mapping) throws
    Exception {
    element.setGraphIds(mapping.f1);
    return element;
  }
}
