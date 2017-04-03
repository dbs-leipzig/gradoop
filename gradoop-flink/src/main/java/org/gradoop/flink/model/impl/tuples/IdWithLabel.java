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

package org.gradoop.flink.model.impl.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * (Id, Label)
 *
 * f0: id
 * f1: label
 */
public class IdWithLabel extends Tuple2<GradoopId, String> {

  /**
   * Constructor.
   *
   * @param id element id
   * @param label element label
   */
  public IdWithLabel(GradoopId id, String label) {
    super(id, label);
  }

  /**
   * Default Constructor
   */
  public IdWithLabel() {
  }

  public void setId(GradoopId id) {
    f0 = id;
  }

  public GradoopId getId() {
    return f0;
  }

  public void setLabel(String label) {
    f1 = label;
  }

  public String getLabel() {
    return f1;
  }
}
