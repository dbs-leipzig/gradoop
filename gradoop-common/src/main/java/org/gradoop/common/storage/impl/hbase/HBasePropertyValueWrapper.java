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

package org.gradoop.common.storage.impl.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.DateTimeSerializer;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wraps a property value to implement HBase's Writable interface.
 */
public class HBasePropertyValueWrapper implements Writable {

  /**
   * Wrapped value.
   */
  private final PropertyValue value;

  /**
   * Constructor.
   *
   * @param value value to wrap
   */
  public HBasePropertyValueWrapper(PropertyValue value) {
    this.value = value;
  }

  /**
   * Byte representation:
   *
   * byte 1       : type info
   *
   * for dynamic length types (e.g. String and BigDecimal)
   * byte 2       : length (short)
   * byte 3       : length (short)
   * byte 4 - end : value bytes
   *
   * for fixed length types (e.g. int, long, float, ...)
   * byte 2 - end : value bytes
   *
   * @param dataOutput data output to write data to
   * @throws IOException
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    byte[] rawBytes = value.getRawBytes();

    // null?
    // type
    dataOutput.writeByte(rawBytes[0]);
    // dynamic type?
    if (rawBytes[0] == PropertyValue.TYPE_STRING || rawBytes[0] == PropertyValue.TYPE_BIG_DECIMAL ||
      rawBytes[0] == PropertyValue.TYPE_MAP || rawBytes[0] == PropertyValue.TYPE_LIST) {
      // write length
      dataOutput.writeShort(rawBytes.length - PropertyValue.OFFSET);
    }
    // write data
    dataOutput.write(rawBytes, PropertyValue.OFFSET, rawBytes.length - PropertyValue.OFFSET);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    short length = 0;
    // type
    byte type = dataInput.readByte();
    // dynamic type?
    if (type == PropertyValue.TYPE_STRING || type == PropertyValue.TYPE_BIG_DECIMAL ||
      type == PropertyValue.TYPE_MAP || type == PropertyValue.TYPE_LIST) {
      // read length
      length = dataInput.readShort();
    } else if (type == PropertyValue.TYPE_NULL) {
      length = 0;
    } else if (type == PropertyValue.TYPE_BOOLEAN) {
      length = Bytes.SIZEOF_BOOLEAN;
    } else if (type == PropertyValue.TYPE_INTEGER) {
      length = Bytes.SIZEOF_INT;
    } else if (type == PropertyValue.TYPE_LONG) {
      length = Bytes.SIZEOF_LONG;
    } else if (type == PropertyValue.TYPE_FLOAT) {
      length = Bytes.SIZEOF_FLOAT;
    } else if (type == PropertyValue.TYPE_DOUBLE) {
      length = Bytes.SIZEOF_DOUBLE;
    } else if (type == PropertyValue.TYPE_GRADOOP_ID) {
      length = GradoopId.ID_SIZE;
    } else if (type == PropertyValue.TYPE_DATE) {
      length = DateTimeSerializer.SIZEOF_DATE;
    } else if (type == PropertyValue.TYPE_TIME) {
      length = DateTimeSerializer.SIZEOF_TIME;
    } else if (type == PropertyValue.TYPE_DATETIME) {
      length = DateTimeSerializer.SIZEOF_DATETIME;
    }
    // init new array
    byte[] rawBytes = new byte[PropertyValue.OFFSET + length];
    // read type info
    rawBytes[0] = type;
    // read data
    for (int i = PropertyValue.OFFSET; i < rawBytes.length; i++) {
      rawBytes[i] = dataInput.readByte();
    }

    value.setBytes(rawBytes);
  }
}
