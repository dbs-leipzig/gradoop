package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.factories.LabelPairCellValueFactory;
import org.gradoop.flink.algorithms.fsm.transactional.common.tuples.LabelPair;
import org.gradoop.flink.model.api.pojos.AdjacencyListCellValueFactory;
import org.gradoop.flink.representation.transactional.RepresentationConverters;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;


public class ToAdjacencyList
  implements MapFunction<GraphTransaction, AdjacencyList<LabelPair>> {

  private AdjacencyListCellValueFactory<LabelPair> cellValueFactory =
    new LabelPairCellValueFactory();

  @Override
  public AdjacencyList<LabelPair> map(GraphTransaction graphTransaction) throws Exception {

    AdjacencyList<LabelPair> adjacencyList =
      RepresentationConverters.getAdjacencyList(graphTransaction, cellValueFactory);
    return adjacencyList;
  }
}
