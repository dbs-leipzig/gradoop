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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

import java.util.List;

/**
 * Takes an EPGM edge as input and creates a {@link EdgeGroupItem} which
 * contains only necessary information for further processing.
 *
 */
@FunctionAnnotation.ForwardedFields("sourceId->f0;targetId->f1;")
@FunctionAnnotation.ReadFields("label;properties")
public class BuildEdgeGroupItem
  extends BuildGroupItemBase
  implements FlatMapFunction<Edge, EdgeGroupItem> {

  /**
   * Avoid object initialization in each call.
   */
  private final EdgeGroupItem reuseEdgeGroupItem;

  /**
   * Creates map function.
   *
//   * @param groupPropertyKeys               vertex property key for grouping
   * @param useLabel                        true, if vertex label shall be used
//   * @param edgeAggregators                 aggregate functions for super edges
   * @param edgeLabelGroups                 stores grouping properties for edge labels
//   * @param labelWithAggregatorPropertyKeys stores all aggregator property keys for each label
   */
  public BuildEdgeGroupItem( boolean useLabel, List<LabelGroup> edgeLabelGroups) {
    super(useLabel, edgeLabelGroups);
//  public BuildEdgeGroupItem(List<String> groupPropertyKeys,
//    boolean useLabel, List<PropertyValueAggregator> edgeAggregators,
//    List<LabelGroup> edgeLabelGroups,
//    Map<String, Set<String>> labelWithAggregatorPropertyKeys) {
//    super(groupPropertyKeys, useLabel, edgeAggregators, labelWithAggregatorPropertyKeys);
    this.reuseEdgeGroupItem = new EdgeGroupItem();
//    if (!doAggregate()) {
//      this.reuseEdgeGroupItem.setAggregateValues(
//        PropertyValueList.createEmptyList());
//    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(Edge edge, Collector<EdgeGroupItem> collector) throws Exception {
//    List<PropertyValue> values = Lists.newArrayList();
    boolean usedEdgeLabelGroup = false;

    reuseEdgeGroupItem.setSourceId(edge.getSourceId());
    reuseEdgeGroupItem.setTargetId(edge.getTargetId());
//    reuseEdgeGroupItem.setGroupLabel(edge.getLabel());
//    if (doAggregate()) {
//      reuseEdgeGroupItem.setAggregateValues(getAggregateValues(edge));
//    }

    // check if edge shall be grouped by a special set of keys
    for (LabelGroup edgeLabelGroup : getLabelGroups()) {

      if (edgeLabelGroup.getGroupingLabel().equals(edge.getLabel()) ) {
        usedEdgeLabelGroup = true;
        // add value for grouping if exist
//        for (String groupPropertyKey : edgeLabelGroup.getPropertyKeys()) {
//          if (edge.hasProperty(groupPropertyKey)) {
//            values.add(edge.getPropertyValue(groupPropertyKey));
//          } else {
//            values.add(PropertyValue.NULL_VALUE);
//          }
//        }
//        reuseEdgeGroupItem.setLabelGroup(edgeLabelGroup);
//        reuseEdgeGroupItem.setGroupingValues(PropertyValueList.fromPropertyValues(values));

        setGroupItem(reuseEdgeGroupItem, edge, edgeLabelGroup);

        collector.collect(reuseEdgeGroupItem);
//        values.clear();
      }
    }
    // standard grouping case
    if (!usedEdgeLabelGroup) {
//      System.err.println("BUILD EDGEITEM STANDARD GROUPING");
      setGroupItem(reuseEdgeGroupItem, edge, getDefaultLabelGroup());
      collector.collect(reuseEdgeGroupItem);
//      reuseEdgeGroupItem.setLabelGroup(new LabelGroup(edge.getLabel(),
//        getGroupPropertyKeys().toArray(new String[getGroupPropertyKeys().size()])));
//      reuseEdgeGroupItem.setGroupingValues(getGroupProperties(edge));
//      collector.collect(reuseEdgeGroupItem);
    }

  }
}
