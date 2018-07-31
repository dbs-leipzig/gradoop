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
package org.gradoop.flink.model.impl.functions.epgm;

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;

/**
 * Accepts all elements which have a property with the specified key or key value combination.
 *
 * @param <E> EPGM element
 */
public class ByProperty<E extends EPGMElement> implements CombinableFilter<E> {
  /**
   * PropertyKey to be filtered on.
   */
  private String key;
  /**
   * PropertyValue to be filtered on.
   */
  private PropertyValue value;

  /**
   * Valued constructor, accepts all elements containing a property with the given key.
   *
   * @param key property key
   */
  public ByProperty(String key) {
    this(key, null);
  }

  /**
   * Valued constructor, accepts all elements containing the given property.
   *
   * @param property property, containing of key and value
   */
  public ByProperty(Property property) {
    this(property.getKey(), property.getValue());
  }

  /**
   * Valued constructor, accepts all elements containing a property with the given key and the
   * corresponding value.
   *
   * @param key property key
   * @param value property value
   */
  public ByProperty(String key, PropertyValue value) {
    this.key = key;
    this.value = value;
  }


  @Override
  public boolean filter(E e) throws Exception {
    if (e.hasProperty(key)) {
      if (value != null) {
        if (e.getPropertyValue(key).equals(value)) {
          return true;
        }
      } else {
        return true;
      }
    }
    return false;
  }
}
