/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.unary.summarization.tuples;

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Edge representation used in grouping edges to summarized edges.
 *
 * f0: edge id
 * f1: source vertex id
 * f2: target vertex id
 * f3: edge group label
 * f4: edge group property value
 */
public class EdgeGroupItem extends Tuple5<Long, Long, Long, String, String> {

  public Long getEdgeId() {
    return f0;
  }

  public void setEdgeId(Long edgeId) {
    f0 = edgeId;
  }

  public Long getSourceVertexId() {
    return f1;
  }

  public void setSourceVertexId(Long sourceVertexId) {
    f1 = sourceVertexId;
  }

  public Long getTargetVertexId() {
    return f2;
  }

  public void setTargetVertexId(Long targetVertexId) {
    f2 = targetVertexId;
  }

  public String getGroupLabel() {
    return f3;
  }

  public void setGroupLabel(String groupLabel) {
    f3 = groupLabel;
  }

  public String getGroupPropertyValue() {
    return f4;
  }

  public void setGroupPropertyValue(String groupPropertyValue) {
    f4 = groupPropertyValue;
  }
}
