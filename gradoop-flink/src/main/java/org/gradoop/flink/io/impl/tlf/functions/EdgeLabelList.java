
package org.gradoop.flink.io.impl.tlf.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.representation.transactional.GraphTransaction;

/**
 * Collects all edge labels.
 */
public class EdgeLabelList implements
  FlatMapFunction<GraphTransaction, String> {

  @Override
  public void flatMap(GraphTransaction graphTransaction, Collector<String> collector)
    throws Exception {
    for (Edge edge : graphTransaction.getEdges()) {
      collector.collect(edge.getLabel());
    }
  }
}
