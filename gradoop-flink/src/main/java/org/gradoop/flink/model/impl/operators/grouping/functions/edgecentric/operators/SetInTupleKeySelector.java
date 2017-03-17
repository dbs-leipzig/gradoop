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

package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;

import java.util.TreeSet;
import java.util.Set;


public class SetInTupleKeySelector<T extends Tuple, E> implements KeySelector<T, String> {

  private final StringBuilder sb;
  private final TreeSet<E> sortedSet;
  private final int[] fields;

  public SetInTupleKeySelector(int... fields) {
    this.fields = fields;
    sb = new StringBuilder();
    sortedSet = new TreeSet<>();
  }

  @Override
  public String getKey(T tuple) throws Exception {
    sb.setLength(0);

    for (int i = 0; i < fields.length; i++) {
      if (Set.class.isInstance(tuple.getField(fields[i]))) {
        sortedSet.clear();
        sortedSet.addAll(tuple.getField(fields[i]));
        for (E element : sortedSet) {
          sb.append(element.hashCode());
        }
      } else {
        sb.append(tuple.getField(fields[i]).hashCode());
      }
    }
    return sb.toString();
  }
}
