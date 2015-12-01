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

package org.gradoop.model.impl.operators.summarization.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Edge representation used in grouping edges to summarized edges.
 *
 * f0: edge id
 * f1: source vertex id
 * f2: target vertex id
 * f3: edge group label
 * f4: edge group property value
 */
public class EdgeGroupItem
  extends Tuple5<GradoopId, GradoopId, GradoopId, String, PropertyValue> {

  public GradoopId getEdgeId() {
    return f0;
  }

  public void setEdgeId(GradoopId edgeId) {
    f0 = edgeId;
  }

  public GradoopId getSourceId() {
    return f1;
  }

  public void setSourceId(GradoopId sourceVertexId) {
    f1 = sourceVertexId;
  }

  public GradoopId getTargetId() {
    return f2;
  }

  public void setTargetId(GradoopId targetVertexId) {
    f2 = targetVertexId;
  }

  public String getGroupLabel() {
    return f3;
  }

  public void setGroupLabel(String groupLabel) {
    f3 = groupLabel;
  }

  public PropertyValue getGroupPropertyValue() {
    return f4;
  }

  public void setGroupPropertyValue(PropertyValue groupPropertyValue) {
    f4 = groupPropertyValue;
  }
}
