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
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;

import org.gradoop.model.impl.properties.PropertyValueList;

import java.io.IOException;
import java.util.List;

/**
 * Reduces a group of {@link VertexGroupItem} instances.
 */
@FunctionAnnotation.ForwardedFields("f0;f3;f4") // vertexId, label, properties
public class ReduceVertexGroupItems
  extends BuildBase
  implements GroupReduceFunction<VertexGroupItem, VertexGroupItem> {

  /**
   * Reduce object instantiations.
   */
  private final VertexGroupItem reuseVertexGroupItem;

  /**
   * Creates group reduce function.
   *
   * @param useLabel          true, iff labels are used for grouping
   * @param vertexAggregators aggregate functions for super vertices
   */
  public ReduceVertexGroupItems(boolean useLabel,
    List<PropertyValueAggregator> vertexAggregators) {
    super(null, useLabel, vertexAggregators);
    this.reuseVertexGroupItem = new VertexGroupItem();
  }

  @Override
  public void reduce(Iterable<VertexGroupItem> vertexGroupItems,
    Collector<VertexGroupItem> collector) throws Exception {

    GradoopId groupRepresentative         = null;
    String groupLabel                     = null;
    PropertyValueList groupPropertyValues = null;

    boolean firstElement                  = true;

    for (VertexGroupItem groupItem : vertexGroupItems) {
      if (firstElement) {
        groupRepresentative = GradoopId.get();
        groupLabel          = groupItem.getGroupLabel();
        groupPropertyValues = groupItem.getGroupingValues();

        if (useLabel()) {
          reuseVertexGroupItem.setGroupLabel(groupLabel);
        }

        reuseVertexGroupItem.setGroupingValues(groupPropertyValues);
        reuseVertexGroupItem.setGroupRepresentative(groupRepresentative);
        reuseVertexGroupItem.setAggregateValues(groupItem.getAggregateValues());
        reuseVertexGroupItem.setCandidate(groupItem.isCandidate());

        firstElement = false;
      }
      reuseVertexGroupItem.setVertexId(groupItem.getVertexId());

      collector.collect(reuseVertexGroupItem);

      if (doAggregate()) {
        aggregate(groupItem.getAggregateValues());
      }
    }

    // collect single item representing the whole group
    collector.collect(createCandidateTuple(
      groupRepresentative,
      groupLabel,
      groupPropertyValues));

    resetAggregators();
  }

  /**
   * Creates one tuple representing the whole group. This tuple is later
   * used to create a summarized vertex for each group.
   *
   * @param groupRepresentative group representative vertex id
   * @param groupLabel          group label
   * @param groupPropertyValues group property values
   * @return vertex group item
   */
  private VertexGroupItem createCandidateTuple(GradoopId groupRepresentative,
    String groupLabel, PropertyValueList groupPropertyValues)
      throws IOException {
    reuseVertexGroupItem.setVertexId(groupRepresentative);
    reuseVertexGroupItem.setGroupLabel(groupLabel);
    reuseVertexGroupItem.setGroupingValues(groupPropertyValues);
    reuseVertexGroupItem.setAggregateValues(getAggregateValues());
    reuseVertexGroupItem.setCandidate(true);
    return reuseVertexGroupItem;
  }
}
