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
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Wraps a value in a tuple 1.
 *
 * @param <T> value type
 * @deprecated This function is a duplicate of {@link ObjectTo1}, use it instead.
 */
@Deprecated
public class ValueInTuple1<T> implements MapFunction<T, Tuple1<T>> {

  @Override
  public Tuple1<T> map(T value) throws Exception {
    return new Tuple1<>(value);
  }
}
