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

package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.common.model.impl.properties.PropertyValueList;

import java.util.List;

/**
 * Reduces a group of {@link VertexGroupItem} instances.
 */
@FunctionAnnotation.ForwardedFields(
    "f0;" + // vertex id
    "f3;" + // label
    "f4"    // properties
)
public class ReduceVertexGroupItems
  extends ReduceVertexGroupItemBase
  implements GroupReduceFunction<VertexGroupItem, VertexGroupItem> {

  /**
   * Creates group reduce function.
   *
   * @param useLabel          true, iff labels are used for grouping
   * @param vertexAggregators aggregate functions for super vertices
   */
  public ReduceVertexGroupItems(boolean useLabel,
    List<PropertyValueAggregator> vertexAggregators) {
    super(null, useLabel, vertexAggregators);
  }

  @Override
  public void reduce(Iterable<VertexGroupItem> vertexGroupItems,
    Collector<VertexGroupItem> collector) throws Exception {

    GradoopId superVertexId               = null;
    String groupLabel                     = null;
    PropertyValueList groupPropertyValues = null;

    VertexGroupItem reuseTuple = getReuseVertexGroupItem();

    boolean isFirst = true;

    for (VertexGroupItem groupItem : vertexGroupItems) {
      if (isFirst) {
        superVertexId       = GradoopId.get();
        groupLabel          = groupItem.getGroupLabel();
        groupPropertyValues = groupItem.getGroupingValues();

        if (useLabel()) {
          reuseTuple.setGroupLabel(groupLabel);
        }

        reuseTuple.setGroupingValues(groupPropertyValues);
        reuseTuple.setSuperVertexId(superVertexId);
        reuseTuple.setAggregateValues(groupItem.getAggregateValues());
        reuseTuple.setSuperVertex(groupItem.isSuperVertex());

        isFirst = false;
      }
      reuseTuple.setVertexId(groupItem.getVertexId());

      // collect updated vertex item
      collector.collect(reuseTuple);

      if (doAggregate()) {
        aggregate(groupItem.getAggregateValues());
      }
    }

    // collect single item representing the whole group
    collector.collect(createSuperVertexTuple(
      superVertexId,
      groupLabel,
      groupPropertyValues));

    resetAggregators();
  }
}
