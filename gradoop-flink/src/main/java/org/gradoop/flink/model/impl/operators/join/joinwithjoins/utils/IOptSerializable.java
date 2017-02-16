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

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Objects;

/**
 * Due to efficiency's sake, we want to express an optional value as a Pair2
 * containing a boolean and a value. Such value must never be null, otherwise
 * the AF hashing function won't work. So, just replace the null value with
 * a default value, and use the first field in order to point out that the
 * element has no meaning
 * @param <K> Serializable argument
 *
 * Created by Giacomo Bergami on 15/02/17.
 */
public abstract class IOptSerializable<K extends Serializable> extends Tuple2<Boolean, K> implements
  Serializable {

  /**
   * Default constructor used by AF
   */
  public IOptSerializable() {

  }

  /**
   * Semantic constructor
   * @param isThereElement    If the element is present or not
   * @param element           The value that has to be stored
   */
  IOptSerializable(Boolean isThereElement, K element) {
    super(isThereElement, element);
  }

  /** This getter…
   * @return  the value
   */
  public K get() {
    return isPresent() ? super.f1 : null;
  }

  /** This getter…
   * @return if the element is present or not
   */
  public boolean isPresent() {
    return super.f0;
  }

  @Override
  public int hashCode() {
    return isPresent() ? (get().hashCode() == 0 ? 1 : get().hashCode()) : 0;
  }

  /**
   * Overridden equality
   * @param o Object to be compared with
   * @return  The equality test result (value by value)
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return isPresent();
    } else {
      if (o.equals(get())) {
        return true;
      } else if (o instanceof IOptSerializable) {
        IOptSerializable<K> tr = (IOptSerializable<K>) o;
        return Objects.equals(tr.get(), get()) && isPresent() == tr.isPresent();
      } else {
        return false;
      }
    }
  }
}
