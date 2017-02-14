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

package org.gradoop.flink.model.impl.operators.join.operators;

import java.io.Serializable;
import java.util.Objects;

/**
 * Created by Giacomo Bergami on 30/01/17.
 */
public class OptSerializable<K extends Serializable> implements Serializable {
  public final boolean isThereElement;
  public final K elem;

  public OptSerializable(boolean isThereElement, K elem) {
    this.isThereElement = isThereElement;
    this.elem = isThereElement ? elem : null;
  }

  public OptSerializable(K elem) {
    this.isThereElement = elem != null;
    this.elem = elem;
  }

  public K get()  {
    return this.elem;
  }

  public boolean isPresent() {
    return isThereElement;
  }

  public static <K extends Serializable> OptSerializable<K> empty() {
    return new OptSerializable<K>(false,null);
  }

  public static <K extends Serializable> OptSerializable<K> value(K val) {
    return new OptSerializable<K>(true,val);
  }

  @Override
  public int hashCode() {
    return elem!=null ? (elem.hashCode() == 0 ? 1 : elem.hashCode()) : 0;
  }

  @Override
  public boolean equals(Object o) {
    if (o==null) return !isThereElement;
    else {
      if (o.equals(elem)) return true;
      else if (o instanceof OptSerializable) {
        OptSerializable<K> tr = (OptSerializable<K>) o;
        return tr != null && Objects.equals(tr.elem, elem) && isThereElement == tr.isThereElement;
      } else return false;
    }
  }

}
