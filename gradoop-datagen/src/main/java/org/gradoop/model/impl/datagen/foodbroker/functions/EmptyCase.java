package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.model.Case;

public class EmptyCase<V extends EPGMVertex, E extends EPGMEdge>
  implements MapFunction<Boolean, Case<V, E>> {

  @Override
  public Case<V, E> map(Boolean aBoolean) throws Exception {
    return new Case<>();
  }
}
