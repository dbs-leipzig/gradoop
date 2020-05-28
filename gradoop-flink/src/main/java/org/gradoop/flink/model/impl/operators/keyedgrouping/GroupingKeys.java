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
package org.gradoop.flink.model.impl.operators.keyedgrouping;

import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.api.entities.Attributed;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.api.entities.Labeled;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.flink.model.api.functions.KeyFunctionWithDefaultValue;
import org.gradoop.flink.model.impl.operators.keyedgrouping.keys.ConstantKeyFunction;
import org.gradoop.flink.model.impl.operators.keyedgrouping.keys.LabelKeyFunction;
import org.gradoop.flink.model.impl.operators.keyedgrouping.labelspecific.LabelSpecificKeyFunction;
import org.gradoop.flink.model.impl.operators.keyedgrouping.keys.PropertyKeyFunction;

import java.util.List;
import java.util.Map;

/**
 * A factory class for creating instances of commonly used grouping key functions.
 */
public final class GroupingKeys {

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
   * @see LabelKeyFunction
   */
  public static <T extends Labeled> KeyFunctionWithDefaultValue<T, String> label() {
    return new LabelKeyFunction<>();
  }

  /**
   * Group elements differently based on their label. Given a map from label to list of key functions,
   * this key function will use certain key functions depending on the label of an element. For labels
   * which are not part of the map, a default group is used. The group is identified by
   * {@link LabelSpecificKeyFunction#DEFAULT_GROUP_LABEL}.
   *
   * @param keysPerLabel A map from label (and {@link LabelSpecificKeyFunction#DEFAULT_GROUP_LABEL default
   * group}) to a list of key functions.
   * @param <T> The type of the elements to group.
   * @return The label-specific key function.
   * @see LabelSpecificKeyFunction
   */
  public static <T extends Element> KeyFunction<T, Tuple> labelSpecific(
    Map<String, List<KeyFunctionWithDefaultValue<T, ?>>> keysPerLabel) {
    return labelSpecific(keysPerLabel, null);
  }

  /**
   * Group elements based on their label and some key functions depending on their label.
   * This is identical to {@link #labelSpecific(Map)} except for an ability to assign new labels to groups.
   *
   * @param keysPerLabel A map from label to list of key functions.
   * @param groupLabels  A map assigning a new label to groups of a certain label. This parameter is optional,
   *                     can be {@code null} and does not have to contain all labels.
   * @param <T> The type of the elements to group.
   * @return The label-specific key function with label updates.
   * @see LabelSpecificKeyFunction
   */
  public static <T extends Element> KeyFunction<T, Tuple> labelSpecific(
    Map<String, List<KeyFunctionWithDefaultValue<T, ?>>> keysPerLabel, Map<String, String> groupLabels) {
    return new LabelSpecificKeyFunction<>(keysPerLabel, groupLabels);
  }

  /**
   * Group by nothing. This will effectively reduce all elements to the same group.
   * Note that this will only be effective if no other key function is used.
   *
   * @param <T> The type of the elements to group.
   * @return A key function extracting the same key for every element.
   * @see ConstantKeyFunction
   */
  public static <T> KeyFunctionWithDefaultValue<T, Boolean> nothing() {
    return new ConstantKeyFunction<>();
  }

  /**
   * Group by a property key.
   *
   * @param key The property key.
   * @param <T> the type of the elements to group.
   * @return The grouping key function extracting the property with that key.
   * @see PropertyKeyFunction
   */
  public static <T extends Attributed> KeyFunctionWithDefaultValue<T, byte[]> property(String key) {
    return new PropertyKeyFunction<>(key);
  }
}
