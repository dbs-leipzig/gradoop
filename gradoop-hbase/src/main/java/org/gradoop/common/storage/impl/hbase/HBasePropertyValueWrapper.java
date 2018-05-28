/**
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
