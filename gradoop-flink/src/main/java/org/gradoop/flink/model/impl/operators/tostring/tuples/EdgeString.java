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
package org.gradoop.flink.model.impl.operators.tostring.tuples;

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * (graphId, sourceId, targetId, sourceLabel, edgeLabel, targetLabel)
 */
public class EdgeString extends Tuple6<GradoopId, GradoopId, GradoopId,
  String, String, String> {

  /**
   * default constructor
   */
  public EdgeString() {
  }

  /**
   * constructor with field values
   * @param graphId graph id
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param label edge label
   */
  public EdgeString(
    GradoopId graphId, GradoopId sourceId, GradoopId targetId, String label) {

    this.f0 = graphId;
    this.f1 = sourceId;
    this.f2 = targetId;
    this.f3 = "";
    this.f4 = label;
    this.f5 = "";
  }

  public GradoopId getGraphId() {
    return this.f0;
  }

  public GradoopId getSourceId() {
    return this.f1;
  }

  public GradoopId getTargetId() {
    return this.f2;
  }

  public String getSourceLabel() {
    return this.f3;
  }

  public void setSourceLabel(String sourceLabel) {
    this.f3 = sourceLabel;
  }

  public String getEdgeLabel() {
    return this.f4;
  }

  public void setEdgeLabel(String label) {
    this.f4 = label;
  }

  public String getTargetLabel() {
    return this.f5;
  }

  public void setTargetLabel(String targetLabel) {
    this.f5 = targetLabel;
  }

  public void setSourceId(GradoopId sourceId) {
    this.f1 = sourceId;
  }

  public void setTargetId(GradoopId targetId) {
    this.f2 = targetId;
  }
}
