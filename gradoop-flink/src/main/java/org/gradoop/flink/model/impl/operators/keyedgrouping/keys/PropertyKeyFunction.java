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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.gradoop.common.model.api.entities.Attributed;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.KeyFunctionWithDefaultValue;

import java.util.Objects;

/**
 * A grouping key function extracting a property value with a certain type.
 *
 * @param <T> The type of the elements to group.
 */
public class PropertyKeyFunction<T extends Attributed> implements KeyFunctionWithDefaultValue<T, byte[]> {

  /**
   * The key of the property to group by.
   */
  private final String propertyKey;

  /**
   * Create a new instance of this key function.
   *
   * @param propertyKey The key of the property to group by.
   */
  public PropertyKeyFunction(String propertyKey) {
    this.propertyKey = Objects.requireNonNull(propertyKey);
  }

  @Override
  public byte[] getKey(T element) {
    final PropertyValue value = element.getPropertyValue(propertyKey);
    return value == null ? PropertyValue.NULL_VALUE.getRawBytes() : value.getRawBytes();
  }

  @Override
  public void addKeyToElement(T element, Object key) {
    if (!(key instanceof byte[])) {
      throw new IllegalArgumentException("Invalid type for key: " + key.getClass().getSimpleName());
    }
    element.setProperty(propertyKey, PropertyValue.fromRawBytes((byte[]) key));
  }

  @Override
  public TypeInformation<byte[]> getType() {
    return TypeInformation.of(byte[].class);
  }

  @Override
  public byte[] getDefaultKey() {
    return PropertyValue.NULL_VALUE.getRawBytes();
  }
}
