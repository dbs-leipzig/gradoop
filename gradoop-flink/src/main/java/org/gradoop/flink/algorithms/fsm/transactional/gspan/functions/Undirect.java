package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.common.tuples.LabelPair;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListRow;

import java.util.Iterator;
import java.util.Map;

public class Undirect implements MapFunction<AdjacencyList<LabelPair>, AdjacencyList<LabelPair>> {

  @Override
  public AdjacencyList<LabelPair> map(AdjacencyList<LabelPair> graph) throws Exception {

    for (Map.Entry<GradoopId, AdjacencyListRow<LabelPair>> row : graph.getRows().entrySet()) {
      Iterator<AdjacencyListCell<LabelPair>> cellIterator = row.getValue().getCells().iterator();

      while (cellIterator.hasNext()) {
        AdjacencyListCell<LabelPair> cell = cellIterator.next();

        if (! cell.isOutgoing()) {
          if (row.getKey().equals(cell.getVertexId())) {
            cellIterator.remove();
          } else {
            cell.setOutgoing(true);
          }
        }
      }
    }

    return graph;
  }
}
