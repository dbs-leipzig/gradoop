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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.logicalgraph.unary.summarization.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Vertex representation which is used as output of group reduce.
 *
 * f0: vertex id
 * f1: group representative vertex id
 * f2: vertex group label
 * f3: vertex group property
 * f4: total group count
 */
public class VertexGroupItem
  extends Tuple5<GradoopId, GradoopId, String, String, Long> {

  /**
   * Creates a vertex group item.
   */
  public VertexGroupItem() {
    setGroupCount(0L);
  }

  public GradoopId getVertexId() {
    return f0;
  }

  public void setVertexId(GradoopId vertexId) {
    f0 = vertexId;
  }

  public GradoopId getGroupRepresentativeVertexId() {
    return f1;
  }

  public void setGroupRepresentativeVertexId(
    GradoopId groupRepresentativeVertexId) {
    f1 = groupRepresentativeVertexId;
  }

  public String getGroupLabel() {
    return f2;
  }

  public void setGroupLabel(String groupLabel) {
    f2 = groupLabel;
  }

  public String getGroupPropertyValue() {
    return f3;
  }

  public void setGroupPropertyValue(String groupPropertyValue) {
    f3 = groupPropertyValue;
  }

  public Long getGroupCount() {
    return f4;
  }

  public void setGroupCount(Long groupCount) {
    f4 = groupCount;
  }

  /**
   * Resets the fields to initial values. This is necessary if the tuples are
   * reused and not all fields are set by a thread.
   */
  public void reset() {
    f0 = null;
    f1 = null;
    f2 = null;
    f3 = null;
    f4 = 0L;
  }
}
