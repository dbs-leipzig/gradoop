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
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
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
  extends BuildGroupItemBase
  implements FlatMapFunction<Vertex, VertexGroupItem> {

  /**
   * Reduce object instantiations.
   */
  private final VertexGroupItem reuseVertexGroupItem;
//  /**
//   * Stores grouping properties and aggregators for vertex labels.
//   */
//  private final List<LabelGroup> labelGroups;
//  /**
//   * Stores the information about the default label group, this is either the vertex or the
//   * edge default label group.
//   */
//  private final String defaultLabelGroup;

  /**
   * Creates map function
   *
   * @param useLabel                        true, if label shall be considered
   * @param vertexLabelGroups               stores grouping properties for vertex labels
   */
  public BuildVertexGroupItem(boolean useLabel, List<LabelGroup> vertexLabelGroups) {
    super(useLabel, vertexLabelGroups);
//    this.labelGroups = vertexLabelGroups;

    this.reuseVertexGroupItem = new VertexGroupItem();
    this.reuseVertexGroupItem.setSuperVertexId(GradoopId.NULL_VALUE);
    this.reuseVertexGroupItem.setSuperVertex(false);


  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(Vertex vertex, Collector<VertexGroupItem> collector) throws Exception {
//    List<PropertyValue> values = Lists.newArrayList();
    boolean usedVertexLabelGroup = false;

    reuseVertexGroupItem.setVertexId(vertex.getId());

    // check if vertex shall be grouped by a special set of keys
    for (LabelGroup vertexLabelGroup : getLabelGroups()) {
      if (vertexLabelGroup.getGroupingLabel().equals(vertex.getLabel())) {
        usedVertexLabelGroup = true;
        // add value for grouping if exist
//        for (String groupPropertyKey : vertexLabelGroup.getPropertyKeys()) {
//          if (vertex.hasProperty(groupPropertyKey)) {
//            values.add(vertex.getPropertyValue(groupPropertyKey));
//          } else {
//            values.add(PropertyValue.NULL_VALUE);
//          }
//        }
//        reuseVertexGroupItem.setGroupLabel(vertexLabelGroup.getGroupLabel());
//        if (doAggregate(vertexLabelGroup.getAggregators())) {
//          reuseVertexGroupItem.setAggregateValues(
//            getAggregateValues(vertex, vertexLabelGroup.getAggregators()));
//        } else {
//          this.reuseVertexGroupItem.setAggregateValues(PropertyValueList.createEmptyList());
//        }
//        reuseVertexGroupItem.setLabelGroup(vertexLabelGroup);
//        reuseVertexGroupItem.setGroupingValues(PropertyValueList.fromPropertyValues(values));

        setGroupItem(reuseVertexGroupItem, vertex, vertexLabelGroup);



        collector.collect(reuseVertexGroupItem);
//        values.clear();
      }
    }
    // standard grouping case
    // TODO check if even needed
    if (!usedVertexLabelGroup) {
      setGroupItem(reuseVertexGroupItem, vertex, getDefaultLabelGroup());
      collector.collect(reuseVertexGroupItem);
//      reuseVertexGroupItem.setLabelGroup(new LabelGroup(vertex.getLabel(), vertex.getLabel(),
//        getGroupPropertyKeys(), ));
//      reuseVertexGroupItem.setGroupingValues(getGroupProperties(vertex));
//      collector.collect(reuseVertexGroupItem);
    }
  }
}
