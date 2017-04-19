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

package org.gradoop.flink.model.impl.operators.neighborhood.keyselector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Returns the id of an epgm element of a specified field of a given tuple.
 *
 * @param <T> type of the tuple
 */
public class IdInTuple<T extends Tuple> implements KeySelector<T, GradoopId> {

  /**
   * Field of the tuple which contains an epgm element to get the key from.
   */
  private int field;

  /**
   * Valued constructor.
   *
   * @param field field of the element to get the key from
   */
  public IdInTuple(int field) {
    this.field = field;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getKey(T tuple) throws Exception {
    return ((EPGMGraphElement)tuple.getField(field)).getId();
  }
}
