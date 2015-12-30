package org.gradoop.model.impl.operators.summarization.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.operators.summarization.functions.aggregation
  .PropertyValueAggregator;
import org.gradoop.model.impl.operators.summarization.tuples.EdgeGroupItem;

import java.util.List;

public class CombineEdgeGroupItems
  extends BuildSummarizedEdge
  implements GroupCombineFunction<EdgeGroupItem, EdgeGroupItem> {
  /**
   * Creates group reducer
   *
   * @param groupPropertyKeys edge property keys
   * @param useLabel          use edge label
   * @param valueAggregator   aggregate function for edge values
   */
  public CombineEdgeGroupItems(List<String> groupPropertyKeys, boolean useLabel,
    PropertyValueAggregator valueAggregator) {
    super(groupPropertyKeys, useLabel, valueAggregator);
  }

  /**
   * Reduces edge group items to a single edge group item and collects it.
   *
   * @param edgeGroupItems  edge group items
   * @param collector       output collector
   * @throws Exception
   */
  @Override
  public void combine(Iterable<EdgeGroupItem> edgeGroupItems,
    Collector<EdgeGroupItem> collector) throws Exception {
    collector.collect(reduceInternal(edgeGroupItems));
    resetAggregator();
  }
}
