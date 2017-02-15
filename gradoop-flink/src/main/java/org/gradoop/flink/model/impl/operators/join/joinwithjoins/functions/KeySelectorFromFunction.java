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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;

import java.io.Serializable;

/**
 * Defines a KeySelector from a user-given function
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
public class KeySelectorFromFunction implements KeySelector<Vertex, Long>, Serializable {

  private final Function<Vertex, Long> function;

  public KeySelectorFromFunction(Function<Vertex, Long> function) {
    this.function = function;
  }

  public KeySelectorFromFunction() {
    this.function = null;
  }

  @Override
  public Long getKey(Vertex value) throws Exception {
    return function == null ? 0L : function.apply(value);
  }
}
