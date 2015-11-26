package org.gradoop.model.impl.functions.filterfunctions;

import org.apache.flink.api.common.functions.FilterFunction;

public class False<G> implements FilterFunction<G> {
  @Override
  public boolean filter(G g) throws Exception {
    return false;
  }
}
