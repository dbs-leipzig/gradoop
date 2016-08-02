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

package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Filters elements if their identifier is not equal to the given identifier.
 *
 * @param <EL> EPGM element type
 */
@FunctionAnnotation.ReadFields("id")
public class ByDifferentId<EL extends Element>
  implements FilterFunction<EL> {

  /**
   * id
   */
  private final GradoopId id;

  /**
   * Creates new filter instance.
   *
   * @param id identifier
   */
  public ByDifferentId(GradoopId id) {
    this.id = id;
  }

  @Override
  public boolean filter(EL element) throws Exception {
    return !element.getId().equals(id);
  }
}
