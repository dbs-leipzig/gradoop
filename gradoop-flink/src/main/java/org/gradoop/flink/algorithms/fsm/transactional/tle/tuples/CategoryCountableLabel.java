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

package org.gradoop.flink.algorithms.fsm.transactional.tle.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.tuples.Countable;

/**
 * (category, label, frequency)
 */
public class CategoryCountableLabel extends Tuple3<String, String, Long>
  implements Categorizable, Countable {

  /**
   * Default constructor.
   */
  public CategoryCountableLabel() {
  }

  /**
   * Constructor.
   *
   * @param category category
   * @param label label
   * @param count count
   */
  public CategoryCountableLabel(String category, String label, Long count) {
    super(category, label, count);
  }

  @Override
  public String getCategory() {
    return f0;
  }

  @Override
  public void setCategory(String category) {
    f0 = category;
  }

  public String getLabel() {
    return f1;
  }

  public void setLabel(String label) {
    f1 = label;
  }

  @Override
  public long getCount() {
    return f2;
  }

  @Override
  public void setCount(long count) {
    f2 = count;
  }
}
