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

import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.types.Value;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * Represents a list of property values.
 */
public class PropertyValueList
  implements Iterable<PropertyValue>, Serializable, Value, Comparable<PropertyValueList> {

  /**
   * Class version for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Property values are stored in their byte representation.
   */
  private byte[] bytes;

  /**
   * Default constructor (used for (de-)serialization.
   */
  public PropertyValueList() {
  }

  /**
   * Creates a value list from the given bytes.
   *
   * @param bytes byte representation
   */
  private PropertyValueList(byte[] bytes) {
    this.bytes = bytes;
  }

  /**
   * Creates a new property value list containing no elements.
   *
   * @return empty property value list
   */
  public static PropertyValueList createEmptyList() {
    return new PropertyValueList(new byte[0]);
  }

  /**
   * Creates a Property value list from a collection of property values.
   *
   * @param propertyValues property values
   *
   * @return property value list containing the given properties
   */
  public static PropertyValueList fromPropertyValues(
    Collection<PropertyValue> propertyValues) throws IOException {

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(byteStream);
    DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

    for (PropertyValue propertyValue : propertyValues) {
      propertyValue.write(outputView);
    }

    outputStream.flush();
    return new PropertyValueList(byteStream.toByteArray());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PropertyValueList)) {
      return false;
    }
    PropertyValueList that = (PropertyValueList) o;
    return Arrays.equals(bytes, that.bytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  public Iterator<PropertyValue> iterator() {
    return new PropertyValueListIterator(bytes);
  }

  @Override
  public void write(DataOutputView dataOutput) throws IOException {
    dataOutput.writeShort(bytes.length);
    dataOutput.write(bytes);
  }

  @Override
  public void read(DataInputView dataInput) throws IOException {
    bytes = new byte[dataInput.readShort()];
    dataInput.readFully(bytes);
  }

  @Override
  public int compareTo(PropertyValueList o) {
    return Bytes.compareTo(bytes, o.bytes);
  }

  @Override
  public String toString() {
    return StringUtils.join(iterator(), ',');
  }

  /**
   * Used to iterate over the stored values.
   */
  private static class PropertyValueListIterator implements Iterator<PropertyValue> {

    /**
     * Used to wrap the byte array of PropertyValueList
     */
    private final ByteArrayInputStream byteStream;

    /**
     * Wraps the {@code byteStream} and is used to access the {@code read}
     * method of PropertyValue.
     */
    private final DataInputStream inputStream;

    /**
     * Creates new iterator
     *
     * @param rawBytes property value list byte representation
     */
    PropertyValueListIterator(byte[] rawBytes) {
      if (rawBytes == null) {
        rawBytes = new byte[0];
      }
      byteStream = new ByteArrayInputStream(rawBytes);
      inputStream = new DataInputStream(byteStream);
    }

    @Override
    public boolean hasNext() {
      boolean hasNext;
      if (byteStream.available() > 0) {
        hasNext = true;
      } else {
        // close the stream if no more data is available
        try {
          inputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
        hasNext = false;
      }
      return hasNext;
    }

    @Override
    public PropertyValue next() {
      PropertyValue nextValue = new PropertyValue();
      try {
        DataInputView inputView = new DataInputViewStreamWrapper(inputStream);
        nextValue.read(inputView);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return nextValue;
    }

    @Override
    public void remove() {
    }
  }
}
