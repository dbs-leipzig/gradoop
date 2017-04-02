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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation
  .PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;

import java.util.List;

/**
 * Creates a minimal representation of vertex data to be used for label specific grouping.
 *
 * The output of that mapper is {@link VertexGroupItem} that contains
 * the vertex id, vertex label, vertex group properties and vertex aggregate
 * properties.
 */
@FunctionAnnotation.ForwardedFields("id->f0")
@FunctionAnnotation.ReadFields("label;properties")
public class BuildVertexGroupItem
  extends BuildBase
  implements FlatMapFunction<Vertex, VertexGroupItem> {

  /**
   * Reduce object instantiations.
   */
  private final VertexGroupItem reuseVertexGroupItem;

  /**
   * Contains vertex labels with associated grouping keys.
   */
  private final List<LabelGroup> vertexLabelGroups;

  /**
   * Creates map function
   *
   * @param groupPropertyKeys vertex property keys
   * @param useLabel          true, if label shall be considered
   * @param vertexAggregators aggregate functions for super vertices
   * @param vertexLabelGroups stores grouping properties for vertex labels
   */
  public BuildVertexGroupItem(List<String> groupPropertyKeys,
    boolean useLabel, List<PropertyValueAggregator> vertexAggregators,
    List<LabelGroup> vertexLabelGroups) {
    super(groupPropertyKeys, useLabel, vertexAggregators);

    this.vertexLabelGroups = vertexLabelGroups;
    this.reuseVertexGroupItem = new VertexGroupItem();
    this.reuseVertexGroupItem.setSuperVertexId(GradoopId.NULL_VALUE);
    this.reuseVertexGroupItem.setSuperVertex(false);
    if (!doAggregate()) {
      this.reuseVertexGroupItem.setAggregateValues(
        PropertyValueList.createEmptyList());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(Vertex vertex, Collector<VertexGroupItem> collector) throws Exception {
    List<PropertyValue> values =
      Lists.newArrayListWithCapacity(vertex.getPropertyCount());
    boolean usedVertexLabelGroup = false;

    reuseVertexGroupItem.setVertexId(vertex.getId());
    reuseVertexGroupItem.setGroupLabel(getLabel(vertex));
    if (doAggregate()) {
      reuseVertexGroupItem.setAggregateValues(getAggregateValues(vertex));
    }
    // check if vertex shall be grouped by a special set of keys
    for (LabelGroup vertexLabelGroup : vertexLabelGroups) {
      if (vertexLabelGroup.getLabel().equals(vertex.getLabel())) {
        usedVertexLabelGroup = true;
        // add value for grouping if exist
        for (String groupPropertyKey : vertexLabelGroup.getPropertyKeys()) {
          if (vertex.hasProperty(groupPropertyKey)) {
            values.add(vertex.getPropertyValue(groupPropertyKey));
          } else {
            values.add(PropertyValue.NULL_VALUE);
          }
        }
        reuseVertexGroupItem.setVertexLabelGroup(vertexLabelGroup);
        reuseVertexGroupItem.setGroupingValues(PropertyValueList.fromPropertyValues(values));
        collector.collect(reuseVertexGroupItem);
        values.clear();
      }
    }
    // standard grouping case
    if (!usedVertexLabelGroup) {
      reuseVertexGroupItem.setVertexLabelGroup(new LabelGroup(vertex.getLabel(),
        getGroupPropertyKeys().toArray(new String[getGroupPropertyKeys().size()])));
      reuseVertexGroupItem.setGroupingValues(getGroupProperties(vertex));
      collector.collect(reuseVertexGroupItem);
    }
  }
}
