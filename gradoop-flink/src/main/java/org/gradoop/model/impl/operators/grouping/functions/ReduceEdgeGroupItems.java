/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.model.impl.operators.grouping.tuples.EdgeGroupItem;

import java.util.List;

/**
 * Creates a new super edge representing an edge group. The edge stores the
 * group label, the group property value and the aggregate values for its group.
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFields("f0->sourceId;f1->targetId")
public class ReduceEdgeGroupItems<E extends EPGMEdge>
  extends BuildSuperEdge
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
   * @param valueAggregators  aggregate functions for edge values
   * @param edgeFactory       edge factory
   */
  public ReduceEdgeGroupItems(List<String> groupPropertyKeys,
    boolean useLabel,
    List<PropertyValueAggregator> valueAggregators,
    EPGMEdgeFactory<E> edgeFactory) {
    super(groupPropertyKeys, useLabel, valueAggregators);
    this.edgeFactory = edgeFactory;
  }

  /**
   * Reduces edge group items to a single edge group item, creates a new
   * super EPGM edge and collects it.
   *
   * @param edgeGroupItems  edge group items
   * @param collector       output collector
   * @throws Exception
   */
  @Override
  public void reduce(Iterable<EdgeGroupItem> edgeGroupItems,
    Collector<E> collector) throws Exception {

    EdgeGroupItem edgeGroupItem = reduceInternal(edgeGroupItems);

    E superEdge = edgeFactory.createEdge(
      edgeGroupItem.getGroupLabel(),
      edgeGroupItem.getSourceId(),
      edgeGroupItem.getTargetId());

    setGroupProperties(superEdge, edgeGroupItem.getGroupingValues());
    setAggregateValues(superEdge);
    resetAggregators();

    collector.collect(superEdge);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<E> getProducedType() {
    return TypeExtractor.createTypeInfo(edgeFactory.getType());
  }
}
