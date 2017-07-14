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

import org.apache.flink.api.java.tuple.Tuple7;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.operators.grouping.tuples.GroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

import java.util.Set;

/**
 * Edge representation used for grouping edges to super edges.
 *
 * f0: vertex ids
 * f1: super vertex id
 * f2: super edge id
 * f3: vertex group label
 * f4: vertex group property values
 * f5: vertex group aggregate values
 * f6: vertex label group
 */
public class SuperVertexGroupItem
  extends Tuple7<Set<GradoopId>, GradoopId, GradoopId, String, PropertyValueList,
  PropertyValueList, LabelGroup>
  implements GroupItem {

  public Set<GradoopId> getVertexIds() {
    return f0;
  }

  public void setVertexIds(Set<GradoopId> vertexIds) {
    f0 = vertexIds;
  }

  public GradoopId getSuperVertexId() {
    return f1;
  }

  public void setSuperVertexId(GradoopId vertexId) {
    f1 = vertexId;
  }

  public GradoopId getSuperEdgeId() {
    return f2;
  }

  public void setSuperEdgeId(GradoopId edgeId) {
    f2 = edgeId;
  }

  @Override
  public String getGroupLabel() {
    return f3;
  }

  @Override
  public void setGroupLabel(String groupLabel) {
    f3 = groupLabel;
  }

  @Override
  public LabelGroup getLabelGroup() {
    return f6;
  }

  @Override
  public void setLabelGroup(LabelGroup labelGroup) {
    f6 = labelGroup;
  }

  @Override
  public PropertyValueList getGroupingValues() {
    return f4;
  }

  @Override
  public void setGroupingValues(PropertyValueList groupPropertyValues) {
    f4 = groupPropertyValues;
  }

  @Override
  public PropertyValueList getAggregateValues() {
    return f5;
  }

  @Override
  public void setAggregateValues(PropertyValueList value) {
    this.f5 = value;
  }
}
