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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.operators.summarization.tuples.VertexForGrouping;
import org.gradoop.model.impl.operators.summarization.tuples.VertexGroupItem;

/**
 * Creates a single {@link VertexGroupItem} for each group element
 * containing the vertex id and the id of the group representative (the
 * first tuple in the input iterable.
 *
 * Creates one {@link VertexGroupItem} for the whole group that contains the
 * vertex id of the group representative, the group label, the group
 * property value and the total number of group elements.
 */
@FunctionAnnotation.ForwardedFields("f0;f1->f2;f2->f3")
public class VertexGroupReducer implements
  GroupReduceFunction<VertexForGrouping, VertexGroupItem> {
  /**
   * Reduce object instantiations.
   */
  private final VertexGroupItem reuseVertexGroupItem;

  /**
   * Creates group reduce function.
   */
  public VertexGroupReducer() {
    reuseVertexGroupItem = new VertexGroupItem();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reduce(Iterable<VertexForGrouping> groupVertices,
    Collector<VertexGroupItem> collector) throws Exception {
    Long groupRepresentativeVertexId = null;
    long groupCount = 0L;
    String groupLabel = null;
    String groupPropertyValue = null;
    boolean firstElement = true;

    for (VertexForGrouping vertexForGrouping : groupVertices) {
      if (firstElement) {
        // take final group representative vertex id from first tuple
        groupRepresentativeVertexId = vertexForGrouping.getVertexId();
        groupLabel = vertexForGrouping.getGroupLabel();
        groupPropertyValue = vertexForGrouping.getGroupPropertyValue();
        firstElement = false;
      }
      // no need to set group label / property value for those tuples
      reuseVertexGroupItem.setVertexId(vertexForGrouping.getVertexId());
      reuseVertexGroupItem
        .setGroupRepresentativeVertexId(groupRepresentativeVertexId);
      collector.collect(reuseVertexGroupItem);
      groupCount++;
    }

    createGroupRepresentativeTuple(groupRepresentativeVertexId, groupLabel,
      groupPropertyValue, groupCount);
    collector.collect(reuseVertexGroupItem);
    reuseVertexGroupItem.reset();
  }

  /**
   * Creates one tuple representing the whole group. This tuple is later
   * used to create a summarized vertex for each group.
   *
   * @param groupRepresentative group representative vertex id
   * @param groupLabel          group label
   * @param groupPropertyValue  group property value
   * @param groupCount          total group count
   */
  private void createGroupRepresentativeTuple(Long groupRepresentative,
    String groupLabel, String groupPropertyValue, long groupCount) {
    reuseVertexGroupItem.setVertexId(groupRepresentative);
    reuseVertexGroupItem.setGroupLabel(groupLabel);
    reuseVertexGroupItem.setGroupPropertyValue(groupPropertyValue);
    reuseVertexGroupItem.setGroupCount(groupCount);
  }
}
