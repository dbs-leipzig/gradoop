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
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type
 * {@code Set}.
 */
public class SetStrategy extends AbstractVariableSizedPropertyValueStrategy<Set<PropertyValue>> {

  @Override
  public Set<PropertyValue> read(DataInputView inputView, byte typeByte) throws IOException {
    int length;
    // read length
    if ((typeByte & PropertyValue.FLAG_LARGE) == PropertyValue.FLAG_LARGE) {
      length = inputView.readInt();
    } else {
      length = inputView.readShort();
    }
    // init new array
    byte[] rawBytes = new byte[length];

    for (int i = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }

    PropertyValue entry;
    Set<PropertyValue> set = new HashSet<>();
    ByteArrayInputStream byteStream = new ByteArrayInputStream(rawBytes);
    DataInputStream inputStream = new DataInputStream(byteStream);
    DataInputView internalInputView = new DataInputViewStreamWrapper(inputStream);

    try {
      while (inputStream.available() > 0) {
        entry = new PropertyValue();
        entry.read(internalInputView);

        set.add(entry);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading PropertyValue", e);
    }

    return set;
  }

  @Override
  public int compare(Set value, Object other) {
    throw new UnsupportedOperationException("Method compareTo() is not supported for Set.");
  }

  @Override
  public boolean is(Object value) {
    if (!(value instanceof Set)) {
      return false;
    }
    for (Object item : (Set) value) {
      // Set is not a valid property value if it contains any object
      // that is not a property value itself.
      if (!(item instanceof PropertyValue)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public Class<Set<PropertyValue>> getType() {
    return (Class) Set.class;
  }

  @Override
  public Set<PropertyValue> get(byte[] bytes) {
    PropertyValue entry;
    Set<PropertyValue> set = new HashSet<PropertyValue>();
    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
    DataInputStream inputStream = new DataInputStream(byteStream);
    DataInputView internalInputView = new DataInputViewStreamWrapper(inputStream);

    try {
      internalInputView.skipBytesToRead(1);

      while (inputStream.available() > 0) {
        entry = new PropertyValue();
        entry.read(internalInputView);

        set.add(entry);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading PropertyValue", e);
    }

    return set;
  }

  @Override
  public byte getRawType() {
    return PropertyValue.TYPE_SET;
  }

  @Override
  public byte[] getRawBytes(Set<PropertyValue> value) {
    int size = value.stream().mapToInt(PropertyValue::byteSize)
      .sum() + PropertyValue.OFFSET + Bytes.SIZEOF_SHORT;

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
}
