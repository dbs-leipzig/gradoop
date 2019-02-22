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
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.gradoop.common.model.impl.properties.PropertyValue;
import java.io.ByteArrayOutputStream;
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
    DataInputViewStreamWrapper internalInputView = createInputView(rawBytes);
    return createMap(internalInputView);
  }

  @Override
  public int compare(Map value, Object other) {
    throw new UnsupportedOperationException("Method compare() is not supported for Map.");
  }

  @Override
  public boolean is(Object value) {
    if (!(value instanceof Map)) {
      return false;
    }
    for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
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

  /**
   * {@inheritDoc}
   * @throws IOException if converting the byte array to a Map fails.
   */
  @Override
  public Map<PropertyValue, PropertyValue> get(byte[] bytes) throws IOException {
    DataInputViewStreamWrapper inputView = createInputView(bytes);
    Map<PropertyValue, PropertyValue> map;

    try {
      if (inputView.skipBytes(PropertyValue.OFFSET) != PropertyValue.OFFSET) {
        throw new IOException("Malformed entry in PropertyValue Map.");
      }
      map = createMap(inputView);
    } catch (IOException e) {
      throw new IOException("Error while processing DataInputViewStreamWrapper.");
    }

    return map;
  }

  @Override
  public byte getRawType() {
    return PropertyValue.TYPE_MAP;
  }

  /**
   * {@inheritDoc}
   * @throws IOException if converting the value to a byte array fails.
   */
  @Override
  public byte[] getRawBytes(Map<PropertyValue, PropertyValue> value) throws IOException {
    int size = value.keySet().stream().mapToInt(PropertyValue::byteSize).sum() +
        value.values().stream().mapToInt(PropertyValue::byteSize).sum() +
        PropertyValue.OFFSET;

    try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream(size);
        DataOutputStream outputStream = new DataOutputStream(byteStream);
        DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream)) {

      outputStream.write(PropertyValue.TYPE_MAP);
      for (Map.Entry<PropertyValue, PropertyValue> entry : value.entrySet()) {
        entry.getKey().write(outputView);
        entry.getValue().write(outputView);
      }

      return byteStream.toByteArray();
    } catch (IOException e) {
      throw new IOException("Error writing PropertyValue with MapStrategy.", e);
    }
  }

  /**
   * Creates a map with data read from an {@link DataInputViewStreamWrapper}.
   *
   * @param inputView {@link DataInputViewStreamWrapper} containing data
   * @return a map containing the deserialized data
   */
  private Map<PropertyValue, PropertyValue> createMap(DataInputViewStreamWrapper inputView)
    throws IOException {
    PropertyValue key;
    PropertyValue value;

    Map<PropertyValue, PropertyValue> map = new HashMap<>();

    try {
      while (inputView.available() > 0) {
        key = new PropertyValue();
        key.read(inputView);

        value = new PropertyValue();
        value.read(inputView);

        map.put(key, value);
      }
    } catch (IOException e) {
      throw new IOException("Error reading PropertyValue with MapStrategy.", e);
    }
    return map;
  }
}
