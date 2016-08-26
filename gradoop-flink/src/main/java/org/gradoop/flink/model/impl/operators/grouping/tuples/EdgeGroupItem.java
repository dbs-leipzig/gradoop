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

package org.gradoop.flink.model.impl.operators.grouping.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
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
 */
public class EdgeGroupItem
  extends
  Tuple5<GradoopId, GradoopId, String, PropertyValueList, PropertyValueList> {

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
}
