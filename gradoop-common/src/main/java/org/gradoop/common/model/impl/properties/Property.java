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
package org.gradoop.common.model.impl.properties;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A single property in the EPGM model. A property consists of a property key
 * and a property value, whereas a given property key does not enforce specific
 * value type.
 */
public class Property implements Value, Serializable, Comparable<Property> {

  /**
   * Property key
   */
  private String key;

  /**
   * Property value
   */
  private PropertyValue value;

  /**
   * Creates a new property.
   */
  public Property() {
  }

  /**
   * Creates a new property from the given arguments.
   *
   * @param key   property key
   * @param value property value
   */
  Property(String key, PropertyValue value) {
    checkNotNull(key, "Property key was null");
    checkNotNull(value, "Property value was null");
    checkArgument(!key.isEmpty(), "Property key was empty");
    this.key = key;
    this.value = value;
  }

  /**
   * Creates a new property from the given arguments.
   *
   * @param key   property key
   * @param value property value
   * @return property
   */
  public static Property create(String key, PropertyValue value) {
    return new Property(key, value);
  }

  /**
   * Creates a new property from the given arguments. Note that the given
   * value type must be supported by {@link PropertyValue}.
   *
   * @param key   property key
   * @param value property value
   * @return property
   */
  public static Property create(String key, Object value) {
    return new Property(key, PropertyValue.create(value));
  }

  /**
   * Returns the property key.
   *
   * @return property key
   */
  public String getKey() {
    return key;
  }

  /**
   * Sets the property key.
   *
   * @param key property key (must not be {@code null})
   */
  public void setKey(String key) {
    checkNotNull(key, "Property key was null");
    checkArgument(!key.isEmpty(), "Property key was empty");
    this.key = key;
  }

  /**
   * Returns the property value.
   *
   * @return property value
   */
  public PropertyValue getValue() {
    return value;
  }

  /**
   * Sets the property value.
   *
   * @param value property value  (must not be {@code null})
   */
  public void setValue(PropertyValue value) {
    this.value = checkNotNull(value, "Property value was null");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Property property = (Property) o;

    return key.equals(property.key) && value.equals(property.value);
  }

  @Override
  public int hashCode() {
    int result = key.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }

  @Override
  public int compareTo(Property o) {
    return this.getKey().compareTo(o.getKey());
  }

  @Override
  public void write(DataOutputView outputView) throws IOException {
    outputView.writeUTF(key);
    value.write(outputView);
  }

  @Override
  public void read(DataInputView inputView) throws IOException {
    key = inputView.readUTF();
    value = new PropertyValue();
    value.read(inputView);
  }

  @Override
  public String toString() {
    return String.format("%s=%s:%s", key, value, value.getType() != null ?
      value.getType().getSimpleName() : "null");
  }
}
