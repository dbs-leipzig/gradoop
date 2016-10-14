package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by stephan on 14.10.16.
 */
public class EPGMElementToPojo<T extends EPGMElement>
  implements MapFunction<EPGMElement, T> {

  @Override
  public T map(EPGMElement element) throws Exception {
    return (T) element;
  }
}
