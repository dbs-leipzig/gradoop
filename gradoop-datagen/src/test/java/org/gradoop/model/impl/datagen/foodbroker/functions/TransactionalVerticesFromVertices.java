package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.api.EPGMVertex;

/**
 * Created by Stephan on 01.08.16.
 */
public class TransactionalVerticesFromVertices<V extends EPGMVertex>
  implements FilterFunction<V> {

  @Override
  public boolean filter(V v) throws Exception {
    if (v.hasProperty("kind")) {
      if (v.getPropertyValue("kind").toString().equals("TransData")) {
        return true;
      }
    }
    return false;
  }
}
