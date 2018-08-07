/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;

/**
 * Combines a group of {@link EdgeGroupItem} to a single {@link EdgeGroupItem}.
 */
@FunctionAnnotation.ForwardedFields(
    "f0;" + // sourceId
    "f1;" + // targetId
    "f2;" + // group label
    "f3;" + // properties
    "f4;" + // aggregates
    "f5"    // label group
)
public class CombineEdgeGroupItems
  extends BuildSuperEdge
  implements GroupCombineFunction<EdgeGroupItem, EdgeGroupItem> {

  /**
   * Avoid object instantiation.
   */
  private EdgeGroupItem reuseEdgeGroupItem;

  /**
   * Creates group reducer
   *
   * @param useLabel use edge label
   */
  public CombineEdgeGroupItems(boolean useLabel) {
    super(useLabel);
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
    reuseEdgeGroupItem = reduceInternal(edgeGroupItems);
    resetAggregators(reuseEdgeGroupItem.getLabelGroup().getAggregators());
    collector.collect(reuseEdgeGroupItem);
  }
}
