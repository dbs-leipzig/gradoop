package org.gradoop.flink.algorithms.fsm2.factories;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm2.tuples.LabelPair;
import org.gradoop.flink.model.api.pojos.AdjacencyListCellValueFactory;

/**
 * Created by peet on 17.11.16.
 */
public class LabelPairCellValueFactory implements AdjacencyListCellValueFactory<LabelPair> {

  @Override
  public LabelPair createValue(Vertex source, Edge edge, Vertex target) {
    return new LabelPair(edge.getLabel(), target.getLabel());
  }
}
