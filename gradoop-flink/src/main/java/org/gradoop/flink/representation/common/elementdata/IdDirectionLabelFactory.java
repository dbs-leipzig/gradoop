package org.gradoop.flink.representation.common.elementdata;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.representation.transactional.adjacencylist.IdDirectionLabel;

public class IdDirectionLabelFactory implements EdgeDataFactory<IdDirectionLabel> {

  @Override
  public IdDirectionLabel createValue(Edge edge, boolean outgoing) {
    return new IdDirectionLabel(edge.getId(), outgoing, edge.getLabel());
  }
}
