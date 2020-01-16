/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.keyedgrouping.keys;

import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.flink.model.api.functions.KeyFunctionWithDefaultValue;

import java.util.List;

/**
 * A key function that combines other key functions by storing the values of the other key functions
 * in order in a tuple. This key function also provides a default value that contains the default values
 * of all other key functions combined by this function.
 *
 * @param <T> The type of the elements to group.
 */
public class CompositeKeyFunctionWithDefaultValues<T> extends CompositeKeyFunction<T>
  implements KeyFunctionWithDefaultValue<T, Tuple> {

  /**
   * A tuple containing the default values of all key functions combined by this function.
   */
  private final Tuple defaultValue;

  /**
   * Create a new instance of this key function.
   *
   * @param groupingKeyFunctions The key functions to be combined.
   */
  public CompositeKeyFunctionWithDefaultValues(List<KeyFunctionWithDefaultValue<T, ?>> groupingKeyFunctions) {
    super(groupingKeyFunctions);
    defaultValue = Tuple.newInstance(groupingKeyFunctions.size());
    for (int index = 0; index < groupingKeyFunctions.size(); index++) {
      defaultValue.setField(groupingKeyFunctions.get(index).getDefaultKey(), index);
    }
  }

  @Override
  public Tuple getDefaultKey() {
    return defaultValue;
  }
}
