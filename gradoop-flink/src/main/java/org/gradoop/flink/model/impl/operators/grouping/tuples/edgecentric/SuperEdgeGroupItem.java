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
 */
public class SuperEdgeGroupItem
  extends Tuple8<GradoopId, GradoopId, Set<GradoopId>, Set<GradoopId>, String, PropertyValueList,
    PropertyValueList, LabelGroup>
  implements GroupItem {

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

  public GradoopId getSuperEdgeId() {
    return f1;
  }

  public void setSuperEdgeId(GradoopId superEdgeId) {
    f1 = superEdgeId;
  }

  public Set<GradoopId> getSourceIds() {
    return f2;
  }

  public GradoopId getSourceId() {
    return f2.iterator().next();
  }

  public void addSourceId(GradoopId sourceVertexId) {
    f2.add(sourceVertexId);
  }

  public void addSourceIds(Set<GradoopId> sourceVertexIds) {
    f2.addAll(sourceVertexIds);
  }

  public void setSourceIds(Set<GradoopId> sourceVertexIds) {
    f2.clear();
    f2.addAll(sourceVertexIds);
  }

  public void setSourceId(GradoopId sourceVertexId) {
    f2.clear();
    f2.add(sourceVertexId);
  }

  public Set<GradoopId> getTargetIds() {
    return f3;
  }

  public GradoopId getTargetId() {
    return f3.iterator().next();
  }

  public void addTargetId(GradoopId targetVertexId) {
    f3.add(targetVertexId);
  }

  public void addTargetIds(Set<GradoopId> targetVertexIds) {
    f3.addAll(targetVertexIds);
  }

  public void setTargetIds(Set<GradoopId> targetVertexIds) {
    f3.clear();
    f3.addAll(targetVertexIds);
  }

  public void setTargetId(GradoopId targetVertexId) {
    f3.clear();
    f3.add(targetVertexId);
  }

  public String getGroupLabel() {
    return f4;
  }

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

  public PropertyValueList getGroupingValues() {
    return f5;
  }

  public void setGroupingValues(PropertyValueList groupPropertyValues) {
    f5 = groupPropertyValues;
  }

  public PropertyValueList getAggregateValues() {
    return f6;
  }

  public void setAggregateValues(PropertyValueList value) {
    this.f6 = value;
  }

}
