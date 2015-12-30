package org.gradoop.model.impl.operators.summarization.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.impl.operators.summarization.functions.aggregation.PropertyValueAggregator;
import org.gradoop.model.impl.operators.summarization.tuples.EdgeGroupItem;

import java.util.List;

public class ReduceEdgeGroupItems<E extends EPGMEdge>
  extends BuildSummarizedEdge
  implements GroupReduceFunction<EdgeGroupItem, E>, ResultTypeQueryable<E> {

  /**
   * Edge factory.
   */
  private final EPGMEdgeFactory<E> edgeFactory;

  /**
   * Creates group reducer
   *
   * @param groupPropertyKeys edge property keys
   * @param useLabel          use edge label
   * @param valueAggregator   aggregate function for edge values
   * @param edgeFactory       edge factory
   */
  public ReduceEdgeGroupItems(List<String> groupPropertyKeys, boolean useLabel,
    PropertyValueAggregator valueAggregator, EPGMEdgeFactory<E> edgeFactory) {
    super(groupPropertyKeys, useLabel, valueAggregator);
    this.edgeFactory = edgeFactory;
  }

  /**
   * Reduces edge group items to a single edge group item, creates a new
   * summarized EPGM edge and collects it.
   *
   * @param edgeGroupItems  edge group items
   * @param collector       output collector
   * @throws Exception
   */
  @Override
  public void reduce(Iterable<EdgeGroupItem> edgeGroupItems,
    Collector<E> collector) throws Exception {

    EdgeGroupItem edgeGroupItem = reduceInternal(edgeGroupItems);

    E sumEdge = edgeFactory.createEdge(
      edgeGroupItem.getGroupLabel(),
      edgeGroupItem.getSourceId(),
      edgeGroupItem.getTargetId());

    setGroupProperties(sumEdge, edgeGroupItem.getGroupPropertyValues());
    setAggregate(sumEdge);
    resetAggregator();

    collector.collect(sumEdge);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public TypeInformation<E> getProducedType() {
    return (TypeInformation<E>)
      TypeExtractor.createTypeInfo(edgeFactory.getType());
  }
}
