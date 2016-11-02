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

package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.EPGMElement;

/**
 * Filters all elements of the given type.
 */
public class CSVTypeFilter implements FilterFunction<EPGMElement> {
  private Class type;

  /**
   * Creates a filter function.
   *
   * @param type type to be filtered
   */
  public CSVTypeFilter(Class type) {
    this.type = type;
  }

  @Override
  public boolean filter(EPGMElement element) throws Exception {
    if (type.isInstance(element)) {
      return true;
    } else if (type.isInstance(element)) {
      return true;
    } else if (type.isInstance(element)) {
      return true;
    }
    return false;
  }
}