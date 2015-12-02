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

package org.gradoop.model.impl.operators.equality.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.api.EPGMLabeled;
import org.gradoop.model.impl.id.GradoopId;

/**
 * This tuple represents an edge, where label and properties are aggregated
 * into a single string label.
 */
public class EdgeDataLabel
  extends Tuple4<GradoopId, GradoopId, GradoopId, String>
  implements EPGMLabeled {

  /**
   * Default constructor.
   */
  public EdgeDataLabel() {
  }

  /**
   * Constructor with initial values.
   *
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param label edge label
   */
  public EdgeDataLabel(
    GradoopId sourceId, GradoopId targetId, String label) {
    this.f0 = new GradoopId();
    this.f1 = sourceId;
    this.f2 = targetId;
    this.f3 = label;
  }

  public GradoopId getGraphId() {
    return this.f0;
  }

  public void setGraphId(GradoopId graphId) {
    this.f0 = graphId;
  }

  public GradoopId getSourceId() {
    return this.f1;
  }

  public void setSourceId(GradoopId id) {
    this.f1 = id;
  }

  public GradoopId getTargetId() {
    return this.f2;
  }

  public void setTargetId(GradoopId id) {
    this.f2 = id;
  }

  public String getLabel() {
    return this.f3;
  }

  public void setLabel(String label) {
    this.f3 = label;
  }

}
