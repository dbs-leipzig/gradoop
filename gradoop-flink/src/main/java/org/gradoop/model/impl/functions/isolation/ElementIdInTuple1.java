package org.gradoop.model.impl.functions.isolation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 19.11.15.
 */
public class ElementIdInTuple1<E extends EPGMElement>
  implements MapFunction<E, Tuple1<GradoopId>> {

  @Override
  public Tuple1<GradoopId> map(E element) throws Exception {
    return new Tuple1<>(element.getId());
  }
}
