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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.summarization.tuples.VertexGroupItem;
import org.gradoop.model.impl.properties.PropertyValue;
import org.gradoop.model.impl.properties.PropertyValueList;

import java.io.IOException;
import java.util.List;

/**
 * Creates a minimal representation of vertex data to be used for grouping.
 *
 * The output of that mapper is {@link VertexGroupItem} that contains
 * the vertex id, vertex label, vertex properties and an initial group count.
 *
 * @param <V> EPGM vertex type
 */
public class BuildVertexGroupItem<V extends EPGMVertex>
  implements MapFunction<V, VertexGroupItem> {

  /**
   * Vertex property keys that are used for grouping.
   */
  private final List<String> groupPropertyKeys;
  /**
   * True, if vertex label shall be considered.
   */
  private final boolean useLabel;
  /**
   * Reduce object instantiations.
   */
  private final VertexGroupItem reuseVertexGroupItem;

  /**
   * Creates map function
   *
   * @param groupPropertyKeys vertex property keys
   * @param useLabel          true, if label shall be considered
   */
  public BuildVertexGroupItem(List<String> groupPropertyKeys,
    boolean useLabel) {
    this.groupPropertyKeys = groupPropertyKeys;
    this.useLabel = useLabel;
    this.reuseVertexGroupItem = new VertexGroupItem();
    this.reuseVertexGroupItem.setGroupCount(1L);
    this.reuseVertexGroupItem.setGroupRepresentative(new GradoopId());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexGroupItem map(V vertex) throws Exception {
    reuseVertexGroupItem.setVertexId(vertex.getId());
    reuseVertexGroupItem.setGroupLabel(getGroupLabel(vertex));
    reuseVertexGroupItem.setGroupPropertyValues(getPropertyValueList(vertex));
    return reuseVertexGroupItem;
  }

  /**
   * Returns the vertex label or {@code null} if no label is required.
   *
   * @param vertexData vertex data
   * @return vertex label or {@code null} if no label is required
   */
  private String getGroupLabel(V vertexData) {
    return useLabel ? vertexData.getLabel() : null;
  }

  /**
   * Returns a property value list containing all grouping values. If a vertex
   * does not have a value, it is set to {@code PropertyValue.NULL_VALUE}.
   *
   * @param vertex vertex
   * @return property value list
   */
  private PropertyValueList getPropertyValueList(V vertex) throws IOException {
    List<PropertyValue> values =
      Lists.newArrayListWithCapacity(vertex.getPropertyCount());

    for (String groupPropertyKey : groupPropertyKeys) {
      if (vertex.hasProperty(groupPropertyKey)) {
        values.add(vertex.getPropertyValue(groupPropertyKey));
      } else {
        values.add(PropertyValue.NULL_VALUE);
      }
    }

    return PropertyValueList.fromPropertyValues(values);
  }
}
