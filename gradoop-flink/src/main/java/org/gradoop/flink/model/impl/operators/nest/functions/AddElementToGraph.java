package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;

/**
 * Created by vasistas on 06/04/17.
 */
public class AddElementToGraph<E extends GraphElement>
  implements CrossFunction<E, Tuple2<GradoopId, GradoopId>, E> {
  @Override
  public E cross(E e, Tuple2<GradoopId, GradoopId> gradoopIdGradoopIdTuple2) throws Exception {
    if (e.getGraphIds().contains(gradoopIdGradoopIdTuple2.f1)) {
      e.addGraphId(gradoopIdGradoopIdTuple2.f0);
    }
    return e;
  }
}
