/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.common.model.impl.properties.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.properties.PropertyValue;
import java.io.IOException;

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type
 * {@code String}.
 */
public class StringStrategy extends AbstractVariableSizedPropertyValueStrategy<String> {

  @Override
  public String read(DataInputView inputView, byte typeByte) throws IOException {
    byte[] rawBytes = readVariableSizedData(inputView, typeByte);
    return Bytes.toString(rawBytes);
  }

  @Override
  public int compare(String value, Object other) {
    if (other instanceof String) {
      return value.compareTo((String) other);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof String;
  }

  @Override
  public Class<String> getType() {
    return String.class;
  }

  @Override
  public String get(byte[] bytes) {
    return Bytes.toString(bytes, PropertyValue.OFFSET, bytes.length - PropertyValue.OFFSET);
  }

  @Override
  public byte getRawType() {
    return PropertyValue.TYPE_STRING;
  }

  @Override
  public byte[] getRawBytes(String value) {
    byte[] valueBytes = Bytes.toBytes(value);
    byte[] rawBytes = new byte[PropertyValue.OFFSET + valueBytes.length];
    rawBytes[0] = getRawType();
    Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
    return rawBytes;
  }
}
