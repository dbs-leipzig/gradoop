/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;

/**
 * Creates a global {@link SuperEdgeGroupItem} which represents one group of super edges based on
 * the previously combined {@link SuperEdgeGroupItem}s.
 */

public class ReduceCombinedSuperEdgeGroupItems
  extends ReduceSuperEdgeGroupItemBase
  implements GroupReduceFunction<SuperEdgeGroupItem, SuperEdgeGroupItem> {

  /**
   * Creates group reduce function.
   *
   * @param useLabel true, iff labels are used for grouping
   * @param sourceSpecificGrouping true if the source vertex shall be considered for grouping
   * @param targetSpecificGrouping true if the target vertex shall be considered for grouping
   */
  public ReduceCombinedSuperEdgeGroupItems(boolean useLabel, boolean sourceSpecificGrouping,
    boolean targetSpecificGrouping) {
    super(useLabel, sourceSpecificGrouping, targetSpecificGrouping);
  }

  @Override
  public void reduce(Iterable<SuperEdgeGroupItem> iterable,
    Collector<SuperEdgeGroupItem> collector) throws Exception {
    boolean isFirst = true;

    for (SuperEdgeGroupItem superEdgeGroupItem : iterable) {
      if (isFirst) {
        setReuseSuperEdgeGroupItem(superEdgeGroupItem);
        isFirst = false;
      } else {
        getReuseSuperEdgeGroupItem().addSourceIds(superEdgeGroupItem.getSourceIds());
        getReuseSuperEdgeGroupItem().addTargetIds(superEdgeGroupItem.getTargetIds());
      }
      if (doAggregate(superEdgeGroupItem.getLabelGroup().getAggregators())) {
        aggregate(
          superEdgeGroupItem.getAggregateValues(),
          getReuseSuperEdgeGroupItem().getLabelGroup().getAggregators());
      }
    }
    getReuseSuperEdgeGroupItem().setAggregateValues(
      getAggregateValues(getReuseSuperEdgeGroupItem().getLabelGroup().getAggregators()));
    collector.collect(getReuseSuperEdgeGroupItem());
  }
}
