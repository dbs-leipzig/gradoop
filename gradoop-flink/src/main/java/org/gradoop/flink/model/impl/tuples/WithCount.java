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

package org.gradoop.flink.model.impl.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (t,count)
 *
 * @param <T> data type of t
 */
public class WithCount<T> extends Tuple2<T, Integer> {

  /**
   * default constructor
   */
  public WithCount() {
  }

  /**
   * valued constructor
   *
   * @param t countable object
   */
  public WithCount(T t) {
    super(t, 1);
  }

  /**
   * valued constructor
   *
   * @param t countable object
   * @param count initial count
   */
  public WithCount(T t, int count) {
    super(t, count);
  }

  public T getObject() {
    return f0;
  }

  public void setCount(T object) {
    this.f0 = object;
  }

  public Integer getCount() {
    return f1;
  }

  public void setCount(int count) {
    this.f1 = count;
  }
}
