package org.gradoop.model.impl.operators.cam.functions;

import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.operators.cam.tuples.EdgeLabel;

/**
 * Created by peet on 03.02.16.
 */
public class EdgeSurpressor<E extends EPGMEdge> implements EdgeLabeler<E> {

  @Override
  public void flatMap(E e, Collector<EdgeLabel> collector) throws Exception {
  }
}
