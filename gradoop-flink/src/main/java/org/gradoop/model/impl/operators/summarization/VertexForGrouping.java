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

package org.gradoop.model.impl.operators.summarization;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Minimalistic representation of a vertex which is used for grouping.
 *
 * Consists of vertex id, vertex label and vertex property value.
 */
public class VertexForGrouping extends Tuple3<Long, String, String> {
  public Long getVertexId() {
    return f0;
  }

  public void setVertexId(Long vertexId) {
    f0 = vertexId;
  }

  public String getGroupLabel() {
    return f1;
  }

  public void setGroupLabel(String groupLabel) {
    f1 = groupLabel;
  }

  public String getGroupPropertyValue() {
    return f2;
  }

  public void setGroupPropertyValue(String groupPropertyValue) {
    f2 = groupPropertyValue;
  }
}
