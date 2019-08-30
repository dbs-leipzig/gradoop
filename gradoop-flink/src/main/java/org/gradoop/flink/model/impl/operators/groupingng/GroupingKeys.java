/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.groupingng;

import org.gradoop.common.model.api.entities.Attributed;
import org.gradoop.common.model.api.entities.Labeled;
import org.gradoop.flink.model.api.functions.GroupingKeyFunction;
import org.gradoop.flink.model.impl.operators.groupingng.keys.LabelKeyFunction;
import org.gradoop.flink.model.impl.operators.groupingng.keys.PropertyKeyFunction;

/**
 * A factory class for creating instances of commonly used grouping key functions.
 */
public class GroupingKeys {

  /**
   * No instances of this class are needed.
   */
  private GroupingKeys() {
  }

  /**
   * Group by label.
   *
   * @param <T> The type of the elements to group.
   * @return The grouping key function extracting the label.
   */
  public static <T extends Labeled> GroupingKeyFunction<T, String> label() {
    return new LabelKeyFunction<>();
  }

  /**
   * Group by a property key.
   *
   * @param key The property key.
   * @param <T> the type of the elements to group.
   * @return The grouping key function extracting the property with that key.
   */
  public static <T extends Attributed> GroupingKeyFunction<T, byte[]> property(String key) {
    return new PropertyKeyFunction<>(key);
  }
}
