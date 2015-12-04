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

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMLabeled;
import org.gradoop.model.impl.id.GradoopId;

/**
 * This tuple represents an graph or vertex, where label, properties (and
 * recursively graph elements) are aggregated into a single string label.
 */
public class DataLabel extends Tuple3<GradoopId, GradoopId, String>
  implements EPGMLabeled {

  /**
   * Default constructor.
   */
  public DataLabel() {
  }

  /**
   * constructor for graph heads
   *
   * @param id graph id
   * @param label graph label
   */
  public DataLabel(GradoopId id, String label) {
    this.f0 = GradoopId.get(); // dummy id to prevent Flink RTE
    this.f1 = id;
    this.f2 = label;
  }

  /**
   * constructor for vertices
   *
   * @param graphId graph id
   * @param vertexId vertex id
   * @param label vertex label
   */
  public DataLabel(GradoopId graphId, GradoopId vertexId, String label) {
    this.f0 = graphId;
    this.f1 = vertexId;
    this.f2 = label;
  }

  public GradoopId getGraphId() {
    return this.f0;
  }

  public void setGraphId(GradoopId id) {
    this.f0 = id;
  }

  public GradoopId getId() {
    return this.f1;
  }

  public void setId(GradoopId id) {
    this.f1 = id;
  }

  public String getLabel() {
    return this.f2;
  }

  public void setLabel(String label) {
    this.f2 = label;
  }

  @Override
  public boolean equals(Object o) {
    boolean isDataLabel = o instanceof EPGMLabeled;

    if (!isDataLabel) {
      return super.equals(o);
    }

    return this.getLabel().equals(((EPGMLabeled) o).getLabel());
  }

  @Override
  public int hashCode() {
    return this.getLabel().hashCode();
  }
}
