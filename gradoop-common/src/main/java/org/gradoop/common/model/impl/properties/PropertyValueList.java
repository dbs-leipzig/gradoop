/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.common.model.impl.properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
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
  implements
  Iterable<PropertyValue>, WritableComparable<PropertyValueList>, Serializable {

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
    DataOutputStream out = new DataOutputStream(byteStream);

    for (PropertyValue propertyValue : propertyValues) {
      propertyValue.write(out);
    }

    out.flush();
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
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeShort(bytes.length);
    dataOutput.write(bytes);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
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
  private static class PropertyValueListIterator
    implements Iterator<PropertyValue> {

    /**
     * Used to wrap the byte array of PropertyValueList
     */
    private final ByteArrayInputStream byteStream;

    /**
     * Wraps the {@code byteStream} and is used to access the {@code readFields}
     * method of PropertyValue.
     */
    private final DataInputStream in;

    /**
     * Creates new iterator
     *
     * @param rawBytes property value list byte representation
     */
    public PropertyValueListIterator(byte[] rawBytes) {
      if (rawBytes == null) {
        rawBytes = new byte[0];
      }
      byteStream = new ByteArrayInputStream(rawBytes);
      in = new DataInputStream(byteStream);
    }

    @Override
    public boolean hasNext() {
      boolean hasNext;
      if (byteStream.available() > 0) {
        hasNext = true;
      } else {
        // close the stream if no more data is available
        try {
          in.close();
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
        nextValue.readFields(in);
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
