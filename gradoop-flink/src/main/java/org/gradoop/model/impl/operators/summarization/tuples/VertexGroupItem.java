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

package org.gradoop.model.impl.operators.summarization.tuples;

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;
import org.gradoop.model.impl.properties.PropertyValueList;

/**
 * Vertex representation which is used as output of group reduce.
 *
 * f0: vertex id
 * f1: group representative vertex id
 * f2: vertex group label
 * f3: vertex group properties
 * f4: vertex group aggregate value
 * f5: candidate tuple yes/no
 */
public class VertexGroupItem
  extends Tuple6
    <GradoopId, GradoopId, String, PropertyValueList, PropertyValue, Boolean> {

  public GradoopId getVertexId() {
    return f0;
  }

  public void setVertexId(GradoopId vertexId) {
    f0 = vertexId;
  }

  public GradoopId getGroupRepresentative() {
    return f1;
  }

  public void setGroupRepresentative(
    GradoopId groupRepresentativeVertexId) {
    f1 = groupRepresentativeVertexId;
  }

  public String getGroupLabel() {
    return f2;
  }

  public void setGroupLabel(String groupLabel) {
    f2 = groupLabel;
  }

  public PropertyValueList getGroupPropertyValues() {
    return f3;
  }

  public void setGroupPropertyValues(PropertyValueList groupPropertyValues) {
    f3 = groupPropertyValues;
  }

  public PropertyValue getGroupAggregate() {
    return f4;
  }

  public void setGroupAggregate(PropertyValue groupCount) {
    f4 = groupCount;
  }

  public Boolean isCandidate() {
    return f5;
  }

  public void setCandidate(Boolean isCandidate) {
    f5 = isCandidate;
  }
}
