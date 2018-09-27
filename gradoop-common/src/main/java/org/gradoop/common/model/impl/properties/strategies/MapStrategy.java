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
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type
 * {@code Map}.
 */
public class MapStrategy implements PropertyValueStrategy<Map> {

  @Override
  public void write(Map value, DataOutputView outputView) throws IOException {
    byte[] rawBytes = getRawBytes(value);
    byte type = rawBytes[0];

    if (rawBytes.length > PropertyValue.LARGE_PROPERTY_THRESHOLD) {
      type |= PropertyValue.FLAG_LARGE;
    }
    outputView.writeByte(type);

    // Write length as an int if the "large" flag is set.
    if ((type & PropertyValue.FLAG_LARGE) == PropertyValue.FLAG_LARGE) {
      outputView.writeInt(rawBytes.length - PropertyValue.OFFSET);
    } else {
      outputView.writeShort(rawBytes.length - PropertyValue.OFFSET);
    }

    outputView.write(rawBytes, PropertyValue.OFFSET, rawBytes.length - PropertyValue.OFFSET);
  }

  @Override
  public Map read(DataInputView inputView, byte typeByte) throws IOException {
    int length;
    // read length
    if ((typeByte & PropertyValue.FLAG_LARGE) == PropertyValue.FLAG_LARGE) {
      length = inputView.readInt();
    } else {
      length = inputView.readShort();
    }

    byte[] rawBytes = new byte[length];

    for (int i = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }

    PropertyValue key;
    PropertyValue value;
    Map<PropertyValue, PropertyValue> map = new HashMap<>();
    ByteArrayInputStream byteStream = new ByteArrayInputStream(rawBytes);
    DataInputStream inputStream = new DataInputStream(byteStream);
    DataInputView internalInputView = new DataInputViewStreamWrapper(inputStream);

    try {
      while (inputStream.available() > 0) {
        key = new PropertyValue();
        key.read(internalInputView);

        value = new PropertyValue();
        value.read(internalInputView);

        map.put(key, value);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading PropertyValue");
    }

    return map;
  }

  @Override
  public int compare(Map value, Object other) {
    throw new UnsupportedOperationException(
      "Method compareTo() is not supported for Map;"
    );
  }

  @Override
  public boolean is(Object value) {
    return value instanceof Map;
  }

  @Override
  public Class<Map> getType() {
    return Map.class;
  }

  @Override
  public Map get(byte[] bytes) {
    PropertyValue key;
    PropertyValue value;

    Map<PropertyValue, PropertyValue> map = new HashMap<PropertyValue, PropertyValue>();

    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
    DataInputStream inputStream = new DataInputStream(byteStream);
    DataInputView inputView = new DataInputViewStreamWrapper(inputStream);

    try {
      if (inputStream.skipBytes(PropertyValue.OFFSET) != PropertyValue.OFFSET) {
        throw new RuntimeException("Malformed entry in PropertyValue List");
      }
      while (inputStream.available() > 0) {
        key = new PropertyValue();
        key.read(inputView);

        value = new PropertyValue();
        value.read(inputView);

        map.put(key, value);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading PropertyValue");
    }

    return map;
  }

  @Override
  public Byte getRawType() {
    return PropertyValue.TYPE_MAP;
  }

  @Override
  public byte[] getRawBytes(Map value) {
    Map<PropertyValue, PropertyValue> map = value;

    int size =
      map.keySet().stream().mapToInt(PropertyValue::byteSize).sum() +
        map.values().stream().mapToInt(PropertyValue::byteSize).sum() +
        PropertyValue.OFFSET;

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(size);
    DataOutputStream outputStream = new DataOutputStream(byteStream);
    DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

    try {
      outputStream.write(PropertyValue.TYPE_MAP);
      for (Map.Entry<PropertyValue, PropertyValue> entry : map.entrySet()) {
        entry.getKey().write(outputView);
        entry.getValue().write(outputView);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error writing PropertyValue");
    }

    return byteStream.toByteArray();
  }
}
