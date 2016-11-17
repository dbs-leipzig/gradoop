package org.gradoop.flink.algorithms.fsm2.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm2.factories.LabelPairCellValueFactory;
import org.gradoop.flink.algorithms.fsm2.tuples.LabelPair;
import org.gradoop.flink.model.api.pojos.AdjacencyListCellValueFactory;
import org.gradoop.flink.representation.RepresentationConverter;
import org.gradoop.flink.representation.tuples.AdjacencyList;
import org.gradoop.flink.representation.tuples.GraphTransaction;


public class ToAdjacencyList
  implements MapFunction<GraphTransaction, AdjacencyList<LabelPair>> {

  private final RepresentationConverter converter = new RepresentationConverter();
  private AdjacencyListCellValueFactory<LabelPair> cellValueFactory =
    new LabelPairCellValueFactory();

  @Override
  public AdjacencyList<LabelPair> map(GraphTransaction graphTransaction) throws Exception {
    return converter.getAdjacencyList(graphTransaction, cellValueFactory);
  }
}
