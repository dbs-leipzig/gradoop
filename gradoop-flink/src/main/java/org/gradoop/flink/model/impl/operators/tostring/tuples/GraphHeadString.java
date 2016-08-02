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

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMLabeled;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * (graphId, label)
 */
public class GraphHeadString extends Tuple2<GradoopId, String>
  implements EPGMLabeled {

  /**
   * default constructor
   */
  public GraphHeadString() {
  }

  /**
   * constructor with field values
   * @param id graph id
   * @param label graph head label
   */
  public GraphHeadString(GradoopId id, String label) {
    this.f0 = id;
    this.f1 = label;
  }

  @Override
  public String getLabel() {
    return this.f1;
  }

  @Override
  public void setLabel(String label) {
    this.f1 = label;
  }
}
