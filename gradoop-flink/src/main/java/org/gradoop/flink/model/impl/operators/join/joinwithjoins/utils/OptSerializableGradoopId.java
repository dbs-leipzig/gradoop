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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils;

import org.gradoop.common.model.impl.id.GradoopId;

/**
 * The type resolutor (the one by string arguments) fails to check for types with parameters for
 * non-default classes (e.g. tuples). So I extend the OptSerializable class in order to avoid
 * such kind of possible problems
 *
 * Created by Giacomo Bergami on 30/01/17.
 */
public class OptSerializableGradoopId extends OptSerializable<GradoopId> implements
  Comparable<OptSerializableGradoopId> {

  /**
   * Default constructor
   * @param isThereElement  If the element is present or not
   * @param elem            If the element is not present, a null is replaced. Please note that, in
   *                        some cases, null could be considered as valid values :O
   */
  private OptSerializableGradoopId(boolean isThereElement, GradoopId elem) {
    super(isThereElement, elem);
  }

  /**
   * Creates an empty element. It'll be hashed to zero. All the other GradoopId with zero
   * hash value are mapped to 1.
   * All the other
   * @return        an instance of an optional value with a missing GradoopId value
   */
  public static  OptSerializableGradoopId empty() {
    return new OptSerializableGradoopId(false, null);
  }

  /**
   * Creates an instance of Optional element with a value. It'll be hashed to a non-zero value.
   * @param val   The GradoopId element
   * @return      an instance of an optional value containing <code>val</code>
   */
  public static OptSerializableGradoopId value(GradoopId val) {
    return new OptSerializableGradoopId(true, val);
  }

  @Override
  public int compareTo(OptSerializableGradoopId o) {
    return (o == null) ?
      1 :
      (isPresent() ?
        (o.isPresent() ? 0 : get().compareTo(o.get())) :
        (o.isPresent() ? -1 : 0)
      );
  }
}
