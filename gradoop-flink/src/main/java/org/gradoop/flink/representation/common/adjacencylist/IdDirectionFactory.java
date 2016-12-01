package org.gradoop.flink.representation.common.adjacencylist;

import org.gradoop.common.model.impl.pojo.Edge;


public class IdDirectionFactory implements EdgeDataFactory<IdDirection> {

  @Override
  public IdDirection createValue(Edge edge, boolean outgoing) {
    return new IdDirection(edge.getId(), outgoing);
  }
}
