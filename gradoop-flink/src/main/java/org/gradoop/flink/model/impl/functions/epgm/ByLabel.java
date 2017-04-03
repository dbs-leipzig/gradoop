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
import org.gradoop.common.model.api.entities.EPGMLabeled;

/**
 * Accepts all elements which have the same label as specified.
 *
 * @param <L> EPGM labeled type
 */
public class ByLabel<L extends EPGMLabeled> implements FilterFunction<L> {
  /**
   * Label to be filtered on.
   */
  private String label;

  /**
   * Valued constructor.
   *
   * @param label label to be filtered on
   */
  public ByLabel(String label) {
    this.label = label;
  }

  @Override
  public boolean filter(L l) throws Exception {
    return l.getLabel().equals(label);
  }
}
