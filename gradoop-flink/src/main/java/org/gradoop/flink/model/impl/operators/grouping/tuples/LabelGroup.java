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

package org.gradoop.flink.model.impl.operators.grouping.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Stores grouping keys for a specific label.
 */
public class LabelGroup extends Tuple2<String, String[]> {

  /**
   * Default constructor.
   */
  public LabelGroup() {
  }

  /**
   * Constructor with varargs.
   *
   * @param label label used for grouping
   * @param propertyKeys variable amount of grouping keys for the label
   */
  public LabelGroup(String label, String... propertyKeys) {
    super(label, propertyKeys);
  }

  public String getLabel() {
    return f0;
  }

  public void setLabel(String label) {
    f0 = label;
  }

  public String[] getPropertyKeys() {
    return f1;
  }

  public void setPropertyKeys(String[] propertyKeys) {
    f1 = propertyKeys;
  }

  /**
   * Creates an empty label group.
   *
   * @return label group
   */
  public static LabelGroup createEmptyLabelGroup() {
    return new LabelGroup("");
  }
}
