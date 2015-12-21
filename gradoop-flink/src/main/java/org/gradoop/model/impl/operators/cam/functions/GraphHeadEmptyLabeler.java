package org.gradoop.model.impl.operators.cam.functions;

import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.operators.cam.tuples.GraphHeadLabel;

public class GraphHeadEmptyLabeler<G extends EPGMGraphHead>
  implements GraphHeadLabeler<G> {

  @Override
  public GraphHeadLabel map(G graphHead) throws Exception {
    return new GraphHeadLabel(graphHead.getId(), "");
  }
}
