/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.api.tuples.Countable;

/**
 * (t,count)
 *
 * f0: object
 * f1: count
 *
 * @param <T> data type of t
 */
public class WithCount<T> extends Tuple2<T, Long> implements Countable {

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
    super(t, 1L);
  }

  /**
   * valued constructor
   *
   * @param t countable object
   * @param count initial count
   */
  public WithCount(T t, long count) {
    super(t, count);
  }

  public T getObject() {
    return f0;
  }

  public void setObject(T object) {
    this.f0 = object;
  }

  @Override
  public long getCount() {
    return f1;
  }

  @Override
  public void setCount(long count) {
    this.f1 = count;
  }
}
