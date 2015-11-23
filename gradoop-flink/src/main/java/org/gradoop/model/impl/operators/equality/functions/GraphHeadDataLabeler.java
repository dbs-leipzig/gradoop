package org.gradoop.model.impl.operators.equality.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;

/**
 * Created by peet on 23.11.15.
 */
public class GraphHeadDataLabeler<G extends EPGMGraphHead>
  extends ElementBaseLabeler
  implements MapFunction<G, DataLabel> {

  @Override
  public DataLabel map(G graphHead) throws Exception {
    String canonicalLabel =
      graphHead.getLabel() + label(graphHead.getProperties());

    return new DataLabel(graphHead.getId(), canonicalLabel);  }
}
