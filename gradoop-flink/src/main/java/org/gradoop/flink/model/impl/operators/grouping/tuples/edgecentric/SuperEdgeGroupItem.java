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

package org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.tuple.Tuple8;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.operators.grouping.tuples.GroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

import java.util.Set;

/**
 * Edge representation used for grouping edges to super edges.
 *
 * f0: edge id
 * f1: super edge id
 * f2: source vertex id
 * f3: target vertex id
 * f4: edge group label
 * f5: edge group property values
 * f6: edge group aggregate values
 * f7: edge label group
 */
public class SuperEdgeGroupItem
  extends Tuple8<GradoopId, GradoopId, Set<GradoopId>, Set<GradoopId>, String, PropertyValueList,
    PropertyValueList, LabelGroup>
  implements GroupItem {

  /**
   * Constructor to initialize sets.
   */
  public SuperEdgeGroupItem() {
    f2 = Sets.newHashSet();
    f3 = Sets.newHashSet();
  }

  public GradoopId getEdgeId() {
    return f0;
  }

  public void setEdgeId(GradoopId edgeId) {
    f0 = edgeId;
  }

  public void setSuperEdgeId(GradoopId superEdgeId) {
    f1 = superEdgeId;
  }

  public GradoopId getSuperEdgeId() {
    return f1;
  }

  public Set<GradoopId> getSourceIds() {
    return f2;
  }

  public GradoopId getSourceId() {
    return f2.iterator().next();
  }

  public void setSourceIds(Set<GradoopId> sourceIds) {
    f2 = sourceIds;
  }

  /**
   * Sets the source id as single element.
   *
   * @param sourceId source id
   */
  public void setSourceId(GradoopId sourceId) {
    f2.clear();
    f2.add(sourceId);
  }

  public Set<GradoopId> getTargetIds() {
    return f3;
  }

  public GradoopId getTargetId() {
    return f3.iterator().next();
  }

  public void setTargetIds(Set<GradoopId> targetIds) {
    f3 = targetIds;
  }

  /**
   * Sets the target id as single element.
   *
   * @param targetId target id
   */
  public void setTargetId(GradoopId targetId) {
    f3.clear();
    f3.add(targetId);
  }

  @Override
  public String getGroupLabel() {
    return f4;
  }

  @Override
  public void setGroupLabel(String groupLabel) {
    f4 = groupLabel;
  }

  @Override
  public LabelGroup getLabelGroup() {
    return f7;
  }

  @Override
  public void setLabelGroup(LabelGroup labelGroup) {
    f7 = labelGroup;
  }

  @Override
  public PropertyValueList getGroupingValues() {
    return f5;
  }

  @Override
  public void setGroupingValues(PropertyValueList groupPropertyValues) {
    f5 = groupPropertyValues;
  }

  @Override
  public PropertyValueList getAggregateValues() {
    return f6;
  }

  @Override
  public void setAggregateValues(PropertyValueList value) {
    this.f6 = value;
  }

}
