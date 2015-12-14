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

package org.gradoop.model.impl.properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * Represents a list of property values.
 */
public class PropertyValueList
  implements Iterable<PropertyValue>, WritableComparable<PropertyValueList> {

  /**
   * Property values are stored in their byte representation.
   */
  private byte[] bytes;

  /**
   * Default constructor.
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
   * Creates a Property value list from a collection of property values.
   *
   * @param propertyValues property values
   *
   * @return property value list containing the given properties
   */
  public static PropertyValueList fromPropertyValues(
    Collection<PropertyValue> propertyValues) {

    PropertyValue[] valueArray = new PropertyValue[propertyValues.size()];
    propertyValues.toArray(valueArray);
    PropertyValueList result = null;
    try {
      result = new PropertyValueList(Writables.getBytes(valueArray));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return result;
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
     * Buffers the byte representation and allows access to the property values
     */
    private final ByteBuffer buffer;

    /**
     * Creates new iterator
     *
     * @param rawBytes property value list byte representation
     */
    public PropertyValueListIterator(byte[] rawBytes) {
      if (rawBytes == null) {
        rawBytes = new byte[0];
      }
      this.buffer = ByteBuffer.wrap(rawBytes);
      buffer.rewind();
    }

    @Override
    public boolean hasNext() {
      return buffer.hasRemaining();
    }

    @Override
    public PropertyValue next() {
      short length = buffer.getShort();
      byte[] bytes = new byte[length];
      buffer.get(bytes, 0, length);
      return new PropertyValue(bytes);
    }

    @Override
    public void remove() {
    }
  }
}
