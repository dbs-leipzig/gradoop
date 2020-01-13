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
package org.gradoop.examples.keyedgrouping.functions;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.gradoop.common.model.api.entities.Attributed;
import org.gradoop.flink.model.api.functions.KeyFunctionWithDefaultValue;

/**
 * A custom key-function extracting the {@code birthday} property and rounding it to the next {@code 10}.<p>
 * This key-function is extracting the property and dividing it by {@code 10}, rounding it.<p>
 * We assume that the age divided by {@code 10} fits in a {@link Byte} value (it's range is
 * {@value Byte#MIN_VALUE} to {@value Byte#MAX_VALUE}).
 *
 * @param <E> The type of elements to extract the key from.
 */
public class AgeRoundedTo10<E extends Attributed> implements KeyFunctionWithDefaultValue<E, Byte> {

  /**
   * Extracts the {@code birthday} property from the element and divides it by {@code 10}.
   * The {@link #getDefaultKey() default key} is returned in case the element does not have the property set.
   *
   * @param element The element to extract the key from.
   * @return The property value, divided by {@code 10}, as a {@link Byte}
   */
  @Override
  public Byte getKey(E element) {
    if (!element.hasProperty("birthday")) {
      return getDefaultKey();
    }
    return (byte) (element.getPropertyValue("birthday").getInt() / 10);
  }

  /**
   * {@inheritDoc}
   * <p>
   * We implement this method to store our grouping-key on super-elements. This is useful in most cases,
   * since the user want's to know which group is represented by a super-element. In our case we multiply
   * the key by {@code 10}, since it was divided by {@code 10} in the {@link #getKey(Attributed)} method.<p>
   * The key is stored as a property with key {@code age_rounded}, we also make sure to ignore the default
   * key, since {@code -10} would not be a correct age and is just used internally as the default value.
   *
   * @param element The element where the key should be stored.
   * @param key     The key to store on the element. A {@link Byte} in this case.
   */
  @Override
  public void addKeyToElement(E element, Object key) {
    // Manually checking the type is technically not required, but is recommended to avoid hard to find bugs.
    if (!(key instanceof Byte)) {
      throw new IllegalArgumentException("Invalid type for key; " + key);
    }
    if (getDefaultKey().equals(key)) {
      return;
    }
    final int actualKey = 10 * (byte) key;
    element.setProperty("age_rounded", actualKey);
  }

  /**
   * {@inheritDoc}
   * <p>
   * We have to provide type infos about the keys extracted by this function.<p>
   * Flink provides commonly used (mostly primitive) types in this class. In most other cases the
   * {@link TypeInformation#of(Class)} should suffice.
   *
   * @return The type information about keys extracted by this class, {@link Byte} in this case.
   */
  @Override
  public TypeInformation<Byte> getType() {
    return BasicTypeInfo.BYTE_TYPE_INFO;
  }

  /**
   * {@inheritDoc}
   * <p>
   * We have to provide a default value if we want to use this key-function with label-specific grouping.
   * This value is used for labels other than the one for which this function is to be used.
   *
   * @return A default value, in this case {@code -1}.
   */
  @Override
  public Byte getDefaultKey() {
    return -1;
  }
}
