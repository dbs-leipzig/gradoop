package org.gradoop.model.impl.functions.isolation;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 19.11.15.
 */
public class ElementIdOnly<E extends EPGMElement>
  implements MapFunction<E, GradoopId> {

  @Override
  public GradoopId map(E element) throws Exception {
    return element.getId();
  }
}