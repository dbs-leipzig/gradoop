package org.gradoop.model.impl.operators.equality.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;

public class VertexDataLabeler<V extends EPGMVertex>
  extends ElementBaseLabeler
  implements MapFunction<V, DataLabel> {

  @Override
  public DataLabel map(V v) throws Exception {

    String canonicalLabel = v.getLabel() + label(v.getProperties());

    return new DataLabel(v.getId(), canonicalLabel);
  }
}
