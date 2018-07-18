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

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValueList;

/**
 * Edge representation used for grouping edges to super edges.
 *
 * f0: source vertex id
 * f1: target vertex id
 * f2: edge group label
 * f3: edge group property values
 * f4: edge group aggregate values
 * f5: edge label group
 */
public class EdgeGroupItem
  extends Tuple6<GradoopId, GradoopId, String, PropertyValueList, PropertyValueList, LabelGroup>
  implements GroupItem {

  public GradoopId getSourceId() {
    return f0;
  }

  public void setSourceId(GradoopId sourceVertexId) {
    f0 = sourceVertexId;
  }

  public GradoopId getTargetId() {
    return f1;
  }

  public void setTargetId(GradoopId targetVertexId) {
    f1 = targetVertexId;
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

  public void setAggregateValues(PropertyValueList value) {
    this.f4 = value;
  }

  public LabelGroup getLabelGroup() {
    return f5;
  }

  public void setLabelGroup(LabelGroup edgeLabelGroup) {
    f5 = edgeLabelGroup;
  }
}
