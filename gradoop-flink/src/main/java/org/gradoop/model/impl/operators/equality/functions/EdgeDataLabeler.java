package org.gradoop.model.impl.operators.equality.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.operators.equality.tuples.EdgeDataLabel;

/**
 * Created by peet on 23.11.15.
 */
public class EdgeDataLabeler<E extends EPGMEdge>
  extends ElementBaseLabeler
  implements MapFunction<E, EdgeDataLabel> {

  @Override
  public EdgeDataLabel map(E e) throws Exception {
    String canonicalLabel = e.getLabel() + label(e.getProperties()) ;

    return new EdgeDataLabel(
      e.getSourceVertexId(), e.getTargetVertexId(), canonicalLabel);
  }
}
