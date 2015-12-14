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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.summarization.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.summarization.tuples.VertexGroupItem;
import org.gradoop.model.impl.properties.PropertyValueList;

/**
 * Reduces or combines a group of {@link VertexGroupItem} instances.
 */
@FunctionAnnotation.ForwardedFields("f0;f3;f4") // vertexId, label, properties
public class ReduceVertexGroupItem
  implements
  GroupReduceFunction<VertexGroupItem, VertexGroupItem>,
  GroupCombineFunction<VertexGroupItem, VertexGroupItem> {

  /**
   * True, if the vertex label shall be considered.
   */
  private final boolean useLabel;

  /**
   * Reduce object instantiations.
   */
  private final VertexGroupItem reuseVertexGroupItem;

  /**
   * Creates group reduce function.
   *
   * @param useLabel true, if labels are used for grouping
   */
  public ReduceVertexGroupItem(boolean useLabel) {
    this.useLabel = useLabel;
    this.reuseVertexGroupItem = new VertexGroupItem();
  }

  @Override
  public void combine(Iterable<VertexGroupItem> iterable,
    Collector<VertexGroupItem> collector) throws Exception {
    reduceInternal(iterable, collector, true);
  }

  @Override
  public void reduce(Iterable<VertexGroupItem> iterable,
    Collector<VertexGroupItem> collector) throws Exception {
    reduceInternal(iterable, collector, false);
  }

  /**
   * {@inheritDoc}
   */
  public void reduceInternal(Iterable<VertexGroupItem> vertexGroupItems,
    Collector<VertexGroupItem> collector, boolean combine) throws Exception {

    GradoopId groupRepresentative = null;

    long groupCount = 0L;
    String groupLabel = null;
    PropertyValueList groupPropertyValues = null;
    boolean firstElement = true;

    for (VertexGroupItem vertexGroupItem : vertexGroupItems) {
      if (firstElement) {
        groupRepresentative = combine ?
          vertexGroupItem.getGroupRepresentative() : GradoopId.get();
        groupLabel = vertexGroupItem.getGroupLabel();
        groupPropertyValues = vertexGroupItem.getGroupPropertyValues();

        if (useLabel) {
          reuseVertexGroupItem.setGroupLabel(groupLabel);
        }
        reuseVertexGroupItem.setGroupPropertyValues(groupPropertyValues);
        reuseVertexGroupItem.setGroupRepresentative(groupRepresentative);
        firstElement = false;
      }
      reuseVertexGroupItem.setVertexId(vertexGroupItem.getVertexId());

      collector.collect(reuseVertexGroupItem);

      if (combine) {
        groupCount++;
      } else {
        groupCount += vertexGroupItem.getGroupCount();
      }
    }

    createGroupRepresentativeTuple(
      groupRepresentative,
      groupLabel,
      groupPropertyValues,
      groupCount);
    collector.collect(reuseVertexGroupItem);
    reuseVertexGroupItem.reset();
  }

  /**
   * Creates one tuple representing the whole group. This tuple is later
   * used to create a summarized vertex for each group.
   *
   * @param groupRepresentative group representative vertex id
   * @param groupLabel          group label
   * @param groupPropertyValues group property values
   * @param groupCount          total group count
   */
  private void createGroupRepresentativeTuple(GradoopId groupRepresentative,
    String groupLabel, PropertyValueList groupPropertyValues, long groupCount) {
    reuseVertexGroupItem.setVertexId(groupRepresentative);
    reuseVertexGroupItem.setGroupLabel(groupLabel);
    reuseVertexGroupItem.setGroupPropertyValues(groupPropertyValues);
    reuseVertexGroupItem.setGroupCount(groupCount);
  }
}
