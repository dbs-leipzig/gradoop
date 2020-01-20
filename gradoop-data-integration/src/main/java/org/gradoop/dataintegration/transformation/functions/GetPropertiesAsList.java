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
package org.gradoop.dataintegration.transformation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.api.entities.Attributed;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A map and key function used to extract property values of certain keys as a {@link PropertyValueList}.<p>
 * This will take a list of property keys and return a list of property values for each element. The order
 * will be the same for both keys and values.<p>
 * Unset properties will be returned as {@link PropertyValue#NULL_VALUE}.
 *
 * @param <E> The type of elements to extract properties from.
 */
public class GetPropertiesAsList<E extends Attributed> implements MapFunction<E, PropertyValueList>,
  KeySelector<E, PropertyValueList> {

  /**
   * The keys of the property values to extract.
   */
  private final List<String> propertyKeys;

  /**
   * Create a new instance of this key function.
   *
   * @param propertyKeys The keys of the properties to extract.
   */
  public GetPropertiesAsList(List<String> propertyKeys) {
    this.propertyKeys = Objects.requireNonNull(propertyKeys);
  }

  @Override
  public PropertyValueList map(E element) throws IOException {
    List<PropertyValue> values = new ArrayList<>();
    for (String key : propertyKeys) {
      final PropertyValue value = element.getPropertyValue(key);
      values.add(value == null ? PropertyValue.NULL_VALUE : value);
    }
    return PropertyValueList.fromPropertyValues(values);
  }

  @Override
  public PropertyValueList getKey(E element) throws IOException {
    return map(element);
  }
}
