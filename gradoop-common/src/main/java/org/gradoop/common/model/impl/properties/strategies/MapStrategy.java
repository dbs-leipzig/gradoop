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
public class MapStrategy
  extends AbstractVariableSizedPropertyValueStrategy<Map<PropertyValue, PropertyValue>> {

  @Override
  public Map<PropertyValue, PropertyValue> read(DataInputView inputView, byte typeByte)
      throws IOException {
    byte[] rawBytes = readVariableSizedData(inputView, typeByte);

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
      throw new RuntimeException("Error reading PropertyValue", e);
    }

    return map;
  }

  @Override
  public int compare(Map value, Object other) {
    throw new UnsupportedOperationException(
      "Method compareTo() is not supported for Map."
    );
  }

  @Override
  public boolean is(Object value) {
    if (!(value instanceof Map)) {
      return false;
    }
    for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
      // Map is not a valid property value if it contains any object
      // that is not a property value itself.
      if (!(entry.getKey() instanceof PropertyValue) ||
        !(entry.getValue() instanceof PropertyValue)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public Class<Map<PropertyValue, PropertyValue>> getType() {
    return (Class) Map.class;
  }

  @Override
  public Map<PropertyValue, PropertyValue> get(byte[] bytes) {
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
      throw new RuntimeException("Error reading PropertyValue", e);
    }

    return map;
  }

  @Override
  public byte getRawType() {
    return PropertyValue.TYPE_MAP;
  }

  @Override
  public byte[] getRawBytes(Map<PropertyValue, PropertyValue> value) {
    int size = value.keySet().stream().mapToInt(PropertyValue::byteSize).sum() +
        value.values().stream().mapToInt(PropertyValue::byteSize).sum() +
        PropertyValue.OFFSET;

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(size);
    DataOutputStream outputStream = new DataOutputStream(byteStream);
    DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

    try {
      outputStream.write(PropertyValue.TYPE_MAP);
      for (Map.Entry<PropertyValue, PropertyValue> entry : value.entrySet()) {
        entry.getKey().write(outputView);
        entry.getValue().write(outputView);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error writing PropertyValue", e);
    }

    return byteStream.toByteArray();
  }
}
