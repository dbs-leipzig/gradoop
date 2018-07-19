/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.grouping.tuples;

import org.apache.flink.api.java.tuple.Tuple7;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValueList;

/**
 * Vertex representation used for grouping vertices to super vertices.
 *
 * f0: vertex id
 * f1: super vertex id
 * f2: vertex group label
 * f3: vertex group properties
 * f4: vertex group aggregate values
 * f5: super vertex tuple true/false
 * f6: vertex label group
 */
public class VertexGroupItem
  extends Tuple7
  <GradoopId, GradoopId, String, PropertyValueList, PropertyValueList, Boolean, LabelGroup>
  implements GroupItem {

  public GradoopId getVertexId() {
    return f0;
  }

  public void setVertexId(GradoopId vertexId) {
    f0 = vertexId;
  }

  public GradoopId getSuperVertexId() {
    return f1;
  }

  public void setSuperVertexId(GradoopId superVertexId) {
    f1 = superVertexId;
  }

  public String getGroupLabel() {
    return f2;
  }

  public void setGroupLabel(String groupLabel) {
    f2 = groupLabel;
  }

  public PropertyValueList getGroupingValues() {
    return f3;
  }

  public void setGroupingValues(PropertyValueList groupPropertyValues) {
    f3 = groupPropertyValues;
  }

  public PropertyValueList getAggregateValues() {
    return f4;
  }

  public void setAggregateValues(PropertyValueList groupCount) {
    f4 = groupCount;
  }

  public Boolean isSuperVertex() {
    return f5;
  }

  public void setSuperVertex(Boolean isSuperVertex) {
    f5 = isSuperVertex;
  }

  public LabelGroup getLabelGroup() {
    return f6;
  }

  public void setLabelGroup(LabelGroup vertexLabelGroup) {
    f6 = vertexLabelGroup;
  }
}
