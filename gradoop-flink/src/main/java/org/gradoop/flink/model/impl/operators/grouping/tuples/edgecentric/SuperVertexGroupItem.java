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

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValueList;

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
 */
public class SuperVertexGroupItem
  extends Tuple6<Set<GradoopId>, GradoopId, GradoopId, String, PropertyValueList,
  PropertyValueList> {

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

  public String getGroupLabel() {
    return f3;
  }

  public void setGroupLabel(String groupLabel) {
    f3 = groupLabel;
  }

  public PropertyValueList getGroupingValues() {
    return f4;
  }

  public void setGroupingValues(PropertyValueList groupPropertyValues) {
    f4 = groupPropertyValues;
  }

  public PropertyValueList getAggregateValues() {
    return f5;
  }

  public void setAggregateValues(PropertyValueList value) {
    this.f5 = value;
  }
}