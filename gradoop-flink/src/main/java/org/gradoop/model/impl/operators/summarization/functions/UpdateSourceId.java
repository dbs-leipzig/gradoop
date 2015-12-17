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

package org.gradoop.model.impl.operators.summarization.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.operators.summarization.tuples.EdgeGroupItem;
import org.gradoop.model.impl.operators.summarization.tuples.VertexWithRepresentative;
import org.gradoop.model.impl.properties.PropertyValue;
import org.gradoop.model.impl.properties.PropertyValueList;

import java.io.IOException;
import java.util.List;

/**
 * Takes an edge and a tuple (vertex-id, group-representative) as input.
 * Replaces the edge-source-id with the group-representative and outputs
 * projected edge information possibly containing the edge label and a
 * group property.
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFieldsSecond("f1") // vertex id -> source id
public class UpdateSourceId<E extends EPGMEdge>
  implements JoinFunction<E, VertexWithRepresentative, EdgeGroupItem> {

  /**
   * Vertex property key for grouping
   */
  private final List<String> groupPropertyKeys;
  /**
   * True, if vertex label shall be considered.
   */
  private final boolean useLabel;

  /**
   * Avoid object initialization in each call.
   */
  private final EdgeGroupItem reuseEdgeGroupItem;

  /**
   * Creates join function.
   *
   * @param groupPropertyKeys vertex property key for grouping
   * @param useLabel         true, if vertex label shall be used
   */
  public UpdateSourceId(List<String> groupPropertyKeys, boolean useLabel) {
    this.groupPropertyKeys = groupPropertyKeys;
    this.useLabel = useLabel;
    this.reuseEdgeGroupItem = new EdgeGroupItem();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EdgeGroupItem join(
    E edge, VertexWithRepresentative vertexRepresentative) throws Exception {

    String groupLabel = useLabel ? edge.getLabel() : null;

    reuseEdgeGroupItem.setEdgeId(edge.getId());
    reuseEdgeGroupItem.setSourceId(
      vertexRepresentative.getGroupRepresentativeVertexId());
    reuseEdgeGroupItem.setTargetId(edge.getTargetId());
    reuseEdgeGroupItem.setGroupLabel(groupLabel);
    reuseEdgeGroupItem.setGroupPropertyValues(getPropertyValueList(edge));

    return reuseEdgeGroupItem;
  }

  /**
   * Returns a property value list with all group by values. If a vertex does
   * not own a value, it is replaced with the null value.
   *
   * @param edge edge
   * @return property value list
   */
  private PropertyValueList getPropertyValueList(E edge) throws IOException {
    List<PropertyValue> values =
      Lists.newArrayListWithCapacity(edge.getPropertyCount());
    for (String groupPropertyKey : groupPropertyKeys) {
      if (edge.hasProperty(groupPropertyKey)) {
        values.add(edge.getPropertyValue(groupPropertyKey));
      } else {
        values.add(PropertyValue.NULL_VALUE);
      }
    }
    return PropertyValueList.fromPropertyValues(values);
  }
}
