package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.api.EPGMVertex;

/**
 * Created by Stephan on 01.08.16.
 */
public class LabledVerticesLineFromVertices<V extends EPGMVertex>
  implements FilterFunction<V> {

  private String label;

  public LabledVerticesLineFromVertices(String label) {
    this.label = label;
  }

  @Override
  public boolean filter(V v) throws Exception {
    return v.getLabel().equals(label);
  }
}
