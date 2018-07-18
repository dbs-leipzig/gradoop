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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.common.model.impl.properties.PropertyValueList;

/**
 * Reduces a group of {@link VertexGroupItem} instances.
 */
@FunctionAnnotation.ForwardedFields(
    "f0;" + // vertex id
    "f2;" + // label
    "f3;" + // properties
    "f4;" + // aggregates
    "f6"    // label group
)
public class ReduceVertexGroupItems
  extends ReduceVertexGroupItemBase
  implements GroupReduceFunction<VertexGroupItem, VertexGroupItem> {

  /**
   * Creates group reduce function.
   *
   * @param useLabel true, iff labels are used for grouping
   */
  public ReduceVertexGroupItems(boolean useLabel) {
    super(useLabel);
  }

  @Override
  public void reduce(Iterable<VertexGroupItem> vertexGroupItems,
    Collector<VertexGroupItem> collector) throws Exception {

    GradoopId superVertexId                         = null;
    String groupLabel                               = null;
    PropertyValueList groupPropertyValues           = null;

    VertexGroupItem reuseTuple = getReuseVertexGroupItem();

    boolean isFirst = true;

    for (VertexGroupItem groupItem : vertexGroupItems) {
      if (isFirst) {
        superVertexId       = GradoopId.get();
        groupLabel          = groupItem.getGroupLabel();
        groupPropertyValues = groupItem.getGroupingValues();

        reuseTuple.setGroupLabel(groupLabel);
        reuseTuple.setGroupingValues(groupPropertyValues);
        reuseTuple.setSuperVertexId(superVertexId);
        reuseTuple.setAggregateValues(groupItem.getAggregateValues());
        reuseTuple.setSuperVertex(groupItem.isSuperVertex());
        reuseTuple.setLabelGroup(groupItem.getLabelGroup());

        isFirst = false;
      }
      reuseTuple.setVertexId(groupItem.getVertexId());

      // collect updated vertex item
      collector.collect(reuseTuple);

      if (doAggregate(groupItem.getLabelGroup().getAggregators())) {
        aggregate(groupItem.getAggregateValues(), reuseTuple.getLabelGroup().getAggregators());
      }
    }

    VertexGroupItem superVertex = createSuperVertexTuple(
      superVertexId,
      groupLabel,
      groupPropertyValues,
      reuseTuple.getLabelGroup().getAggregators());
    resetAggregators(superVertex.getLabelGroup().getAggregators());
    collector.collect(superVertex);
  }
}
