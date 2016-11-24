package org.gradoop.flink.algorithms.fsm.factories;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.tuples.LabelPair;
import org.gradoop.flink.model.api.pojos.AdjacencyListCellValueFactory;

import java.io.Serializable;

/**
 * Created by peet on 17.11.16.
 */
public class LabelPairCellValueFactory
  implements AdjacencyListCellValueFactory<LabelPair>, Serializable {

  @Override
  public LabelPair createValue(Vertex source, Edge edge, Vertex target) {
    return new LabelPair(edge.getLabel(), target.getLabel());
  }
}
