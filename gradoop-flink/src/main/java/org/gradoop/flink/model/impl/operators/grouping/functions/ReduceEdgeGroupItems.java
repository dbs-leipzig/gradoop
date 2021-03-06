/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;

/**
 * Creates a new super edge representing an edge group. The edge stores the
 * group label, the group property value and the aggregate values for its group.
 *
 * @param <E> The edge type.
 */
@FunctionAnnotation.ForwardedFields("f0->sourceId;f1->targetId;f2->label")
@FunctionAnnotation.ReadFields("f3;f5")
public class ReduceEdgeGroupItems<E extends Edge> extends BuildSuperEdge
  implements GroupReduceFunction<EdgeGroupItem, E>, ResultTypeQueryable<E> {

  /**
   * Edge factory.
   */
  private final EdgeFactory<E> edgeFactory;

  /**
   * Creates group reducer
   *
   * @param useLabel use edge label
   * @param edgeFactory edge factory
   */
  public ReduceEdgeGroupItems(boolean useLabel, EdgeFactory<E> edgeFactory) {
    super(useLabel);
    this.edgeFactory = edgeFactory;
  }

  /**
   * Reduces edge group items to a single edge group item, creates a new
   * super edge and collects it.
   *
   * @param edgeGroupItems  edge group items
   * @param collector       output collector
   * @throws Exception on failure
   */
  @Override
  public void reduce(Iterable<EdgeGroupItem> edgeGroupItems, Collector<E> collector) throws Exception {

    EdgeGroupItem edgeGroupItem = reduceInternal(edgeGroupItems);

    E superEdge = edgeFactory.createEdge(
      edgeGroupItem.getGroupLabel(),
      edgeGroupItem.getSourceId(),
      edgeGroupItem.getTargetId());

    setGroupProperties(superEdge, edgeGroupItem.getGroupingValues(), edgeGroupItem.getLabelGroup());
    setAggregateProperties(superEdge, edgeGroupItem.getLabelGroup().getAggregateValueList(),
      edgeGroupItem.getLabelGroup().getAggregateFunctions());
    edgeGroupItem.getLabelGroup().resetAggregateValues();

    collector.collect(superEdge);
  }

  @Override
  public TypeInformation<E> getProducedType() {
    return TypeExtractor.createTypeInfo(edgeFactory.getType());
  }
}
