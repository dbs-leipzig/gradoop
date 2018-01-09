/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

  /**
   * Adds the source ids.
   *
   * @param sourceIds source ids
   */
  public void addSourceIds(Set<GradoopId> sourceIds) {
    f2.addAll(sourceIds);
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

  /**
   * Adds the target ids.
   *
   * @param targetIds target ids
   */
  public void addTargetIds(Set<GradoopId> targetIds) {
    f3.addAll(targetIds);
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
