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

import org.apache.flink.api.java.tuple.Tuple8;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValueList;

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
 * f7: super edge tuple true/false
 */
public class EdgeWithSuperEdgeGroupItem
  extends Tuple8<GradoopId, GradoopId, GradoopId, GradoopId, String, PropertyValueList,
  PropertyValueList, Boolean> {


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

  public GradoopId getSourceId() {
    return f2;
  }

  public void setSourceId(GradoopId sourceVertexId) {
    f2 = sourceVertexId;
  }

  public GradoopId getTargetId() {
    return f3;
  }


  public void setTargetId(GradoopId targetVertexId) {
    f3 = targetVertexId;
  }

  public String getGroupLabel() {
    return f4;
  }

  public void setGroupLabel(String groupLabel) {
    f4 = groupLabel;
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

  public Boolean isSuperEdge() {
    return f7;
  }

  public void setSuperEdge(Boolean isSuperEdge) {
    f7 = isSuperEdge;
  }
}
