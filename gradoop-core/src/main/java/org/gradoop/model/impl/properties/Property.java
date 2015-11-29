/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.properties;

import org.gradoop.model.api.EPGMProperty;
import org.gradoop.model.api.EPGMPropertyValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default property implementation.
 */
public class Property implements EPGMProperty {

  /**
   * Property key
   */
  private String key;

  /**
   * Property value
   */
  private EPGMPropertyValue value;

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
  Property(String key, EPGMPropertyValue value) {
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
  public static Property create(String key, EPGMPropertyValue value) {
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

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public void setKey(String key) {
    checkNotNull(key, "Property key was null");
    checkArgument(!key.isEmpty(), "Property key was empty");
    this.key = key;
  }

  @Override
  public EPGMPropertyValue getValue() {
    return value;
  }

  @Override
  public void setValue(EPGMPropertyValue value) {
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
  public int compareTo(EPGMProperty o) {
    return this.getKey().compareTo(o.getKey());
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(key);
    value.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    key = dataInput.readUTF();
    value = new PropertyValue();
    value.readFields(dataInput);
  }

  @Override
  public String toString() {
    return String.format("%s = %s", key, value);
  }
}
