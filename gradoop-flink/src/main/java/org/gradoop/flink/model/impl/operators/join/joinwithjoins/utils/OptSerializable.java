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

import java.io.Serializable;
import java.util.Objects;

/**
 * Defines an optional value that could be easily serialized. The problem is that Tuples in
 * Apache flink could be hashed iff. there are no null values stored inside. So this is a
 * wrap around, allowing an hashing function even when I have null values. Even by extending
 * the tuple with a different behaviour doesn't work!
 *
 * Created by Giacomo Bergami on 30/01/17.
 *
 * @param <K>   In order to serialize the object, the parameter type should belong to a serializable
 *              object
 */
public class OptSerializable<K extends Serializable> implements Serializable, IOptSerializable {
  /**
   * Checks if the value is present or not.
   */
  private final boolean isThereElement;
  /**
   * The actual value (witness)
   */
  private final K elem;

  /**
   * Default constructor
   * @param isThereElement  If the element is present or not
   * @param elem            If the element is not present, a null is replaced. Please note that, in
   *                        some cases, null could be considered as valid values :O
   */
  OptSerializable(boolean isThereElement, K elem) {
    this.isThereElement = isThereElement;
    this.elem = isThereElement ? elem : null;
  }

  /** This getter…
   * @return  the value
   */
  public K get()  {
    return this.elem;
  }

  /** This getter…
   * @return if the element is present or not
   */
  public boolean isPresent() {
    return isThereElement;
  }

  /**
   * Redefined hash function
   * @return  hash value
   */
  @Override
  public int hashCode() {
    return elem != null ? (elem.hashCode() == 0 ? 1 : elem.hashCode()) : 0;
  }

  /**
   * Overridden equality
   * @param o Object to be compared with
   * @return  The equality test result (value by value)
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return !isThereElement;
    } else {
      if (o.equals(elem)) {
        return true;
      } else if (o instanceof OptSerializable) {
        OptSerializable<K> tr = (OptSerializable<K>) o;
        return Objects.equals(tr.elem, elem) && isThereElement == tr.isThereElement;
      } else {
        return false;
      }
    }
  }

}
