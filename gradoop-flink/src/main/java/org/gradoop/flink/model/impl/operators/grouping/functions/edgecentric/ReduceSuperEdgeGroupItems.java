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

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;

import java.util.Set;

/**
 * Creates a {@link SuperEdgeGroupItem} which represents one group of super edges based on the
 * grouping settings.
 */
@FunctionAnnotation.ForwardedFields(
  "f0;" + // edge id
    "f3;" + // label
    "f4"    // properties
)
@FunctionAnnotation.ReadFields(
  "f1;" + // source
    "f2") // target
public class ReduceSuperEdgeGroupItems
  extends ReduceSuperEdgeGroupItemBase
  implements GroupReduceFunction<SuperEdgeGroupItem, SuperEdgeGroupItem> {

  /**
   * Avoid object instantiation.
   */
  private Set<GradoopId> sources;
  /**
   * Avoid object instantiation.
   */
  private Set<GradoopId> targets;

  /**
   * Creates group reduce function.
   *
   * @param useLabel true, iff labels are used for grouping
   * @param sourceSpecificGrouping true if the source vertex shall be considered for grouping
   * @param targetSpecificGrouping true if the target vertex shall be considered for grouping
   */
  public ReduceSuperEdgeGroupItems(boolean useLabel, boolean sourceSpecificGrouping,
    boolean targetSpecificGrouping) {
    super(useLabel, sourceSpecificGrouping, targetSpecificGrouping);
    sources = Sets.newHashSet();
    targets = Sets.newHashSet();
  }

  @Override
  public void reduce(Iterable<SuperEdgeGroupItem> superEdgeGroupItems,
    Collector<SuperEdgeGroupItem> collector) throws Exception {

    boolean isFirst = true;
    sources.clear();
    targets.clear();

    for (SuperEdgeGroupItem groupItem : superEdgeGroupItems) {
      // grouped by source and target
      if (isSourceSpecificGrouping() && isTargetSpecificGrouping()) {
        if (isFirst) {
          sources.add(groupItem.getSourceId());
          targets.add(groupItem.getTargetId());
        }
      // grouped by source, targets may vary
      } else if (isSourceSpecificGrouping()) {
        if (isFirst) {
          sources.add(groupItem.getSourceId());
        }
        targets.add(groupItem.getTargetId());
      // grouped by target, sources may vary
      } else if (isTargetSpecificGrouping()) {
        if (isFirst) {
          targets.add(groupItem.getTargetId());
        }
        sources.add(groupItem.getSourceId());
      // source or target do not have influence to the grouping
      } else {
        sources.add(groupItem.getSourceId());
        targets.add(groupItem.getTargetId());
      }
      if (isFirst) {
        setReuseSuperEdgeGroupItem(groupItem);
        getReuseSuperEdgeGroupItem().setSuperEdgeId(GradoopId.get());
        isFirst = false;
      }
      if (doAggregate(groupItem.getLabelGroup().getAggregators())) {
        aggregate(
          groupItem.getAggregateValues(),
          getReuseSuperEdgeGroupItem().getLabelGroup().getAggregators());
      }

    }
    // collect single item representing the whole group
    getReuseSuperEdgeGroupItem().setSourceIds(sources);
    getReuseSuperEdgeGroupItem().setTargetIds(targets);
    getReuseSuperEdgeGroupItem().setAggregateValues(
      getAggregateValues(getReuseSuperEdgeGroupItem().getLabelGroup().getAggregators()));
    resetAggregators(getReuseSuperEdgeGroupItem().getLabelGroup().getAggregators());
    collector.collect(getReuseSuperEdgeGroupItem());
  }
}
