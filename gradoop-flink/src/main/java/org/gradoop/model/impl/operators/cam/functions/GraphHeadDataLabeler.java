package org.gradoop.model.impl.operators.cam.functions;

import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.operators.cam.tuples.GraphHeadLabel;

public class GraphHeadDataLabeler<G extends EPGMGraphHead>
  extends EPGMElementDataLabeler<G>
  implements GraphHeadLabeler<G> {

  @Override
  public GraphHeadLabel map(G graph) throws Exception {
    return new GraphHeadLabel(graph.getId(), "|" + label(graph) + "|");
  }
}
