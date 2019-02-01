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
import java.util.ArrayList;
import java.util.List;

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type
 * {@code List}.
 */
public class ListStrategy extends AbstractVariableSizedPropertyValueStrategy<List<PropertyValue>> {

  @Override
  public List<PropertyValue> read(DataInputView inputView, byte typeByte) throws IOException {
    byte[] rawBytes = readVariableSizedData(inputView, typeByte);

    PropertyValue item;
    List<PropertyValue> list = new ArrayList<>();

    ByteArrayInputStream byteStream = new ByteArrayInputStream(rawBytes);
    DataInputStream inputStream = new DataInputStream(byteStream);
    DataInputViewStreamWrapper internalInputView = new DataInputViewStreamWrapper(inputStream);

    try {
      while (internalInputView.available() > 0) {
        item = new PropertyValue();
        item.read(internalInputView);

        list.add(item);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading PropertyValue", e);
    }

    return list;
  }

  @Override
  public int compare(List value, Object other) {
    throw new UnsupportedOperationException(
      "Method compareTo() is not supported for List."
    );
  }

  @Override
  public boolean is(Object value) {
    if (!(value instanceof List)) {
      return false;
    }
    for (Object item : (List) value) {
      // List is not a valid property value if it contains any object
      // that is not a property value itself.
      if (!(item instanceof PropertyValue)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public Class<List<PropertyValue>> getType() {
    return (Class) List.class;
  }

  @Override
  public List<PropertyValue> get(byte[] bytes) {
    PropertyValue item;
    List<PropertyValue> list = new ArrayList<>();

    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
    DataInputStream inputStream = new DataInputStream(byteStream);
    DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

    try {
      if (inputView.skipBytes(PropertyValue.OFFSET) != PropertyValue.OFFSET) {
        throw new RuntimeException("Malformed entry in PropertyValue List");
      }
      while (inputView.available() > 0) {
        item = new PropertyValue();
        item.read(inputView);

        list.add(item);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading PropertyValue", e);
    }

    return list;
  }

  @Override
  public byte getRawType() {
    return PropertyValue.TYPE_LIST;
  }

  @Override
  public byte[] getRawBytes(List<PropertyValue> value) {
    int size = value.stream().mapToInt(PropertyValue::byteSize).sum() +
      PropertyValue.OFFSET;

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(size);
    DataOutputStream outputStream = new DataOutputStream(byteStream);
    DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

    try {
      outputStream.write(getRawType());
      for (PropertyValue entry : value) {
        entry.write(outputView);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error writing PropertyValue", e);
    }

    return byteStream.toByteArray();
  }

  private DataInputView createInputView(byte[] bytes) {
    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
    DataInputStream inputStream = new DataInputStream(byteStream);
    return new DataInputViewStreamWrapper(inputStream);
  }
}
