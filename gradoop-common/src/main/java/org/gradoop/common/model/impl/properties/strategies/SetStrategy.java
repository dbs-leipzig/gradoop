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
    byte[] rawBytes = readVariableSizedData(inputView, typeByte);

    PropertyValue item;
    Set<PropertyValue> set = new HashSet<>();
    DataInputViewStreamWrapper internalInputView = createInputView(rawBytes);

    try {
      while (internalInputView.available() > 0) {
        item = new PropertyValue();
        item.read(internalInputView);

        set.add(item);
      }
    } catch (IOException e) {
      throw new IOException("Error reading PropertyValue with SetStrategy.", e);
    }

    return set;
  }

  @Override
  public int compare(Set value, Object other) {
    throw new UnsupportedOperationException("Method compare() is not supported for Set.");
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

  /**
   * {@inheritDoc}
   * @throws IOException if converting the byte array to a Set fails.
   */
  @Override
  public Set<PropertyValue> get(byte[] bytes) throws IOException {
    PropertyValue entry;
    Set<PropertyValue> set = new HashSet<>();

    try (ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        DataInputStream inputStream = new DataInputStream(byteStream);
        DataInputViewStreamWrapper internalInputView =
           new DataInputViewStreamWrapper(inputStream)) {

      internalInputView.skipBytesToRead(1);

      while (inputStream.available() > 0) {
        entry = new PropertyValue();
        entry.read(internalInputView);

        set.add(entry);
      }
    } catch (IOException e) {
      throw new IOException("Error reading PropertyValue with SetStrategy.", e);
    }

    return set;
  }

  @Override
  public byte getRawType() {
    return PropertyValue.TYPE_SET;
  }

  /**
   * {@inheritDoc}
   * @throws IOException if converting the value to a byte array fails.
   */
  @Override
  public byte[] getRawBytes(Set<PropertyValue> value) throws IOException {
    int size = value.stream().mapToInt(PropertyValue::byteSize)
      .sum() + PropertyValue.OFFSET + Bytes.SIZEOF_SHORT;

    try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream(size);
        DataOutputStream outputStream = new DataOutputStream(byteStream);
        DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream)) {

      outputStream.write(getRawType());

      for (PropertyValue entry : value) {
        entry.write(outputView);
      }

      return byteStream.toByteArray();
    } catch (IOException e) {
      throw new IOException("Error writing PropertyValue with SetStrategy.", e);
    }
  }
}
