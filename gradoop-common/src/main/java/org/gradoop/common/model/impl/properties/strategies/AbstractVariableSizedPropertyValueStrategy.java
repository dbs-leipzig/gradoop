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
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Abstract class that provides generic methods for {@code PropertyValueStrategy} classes that
 * handle data types with a variable size.
 *
 * @param <T> Type with a variable length.
 */
public abstract class AbstractVariableSizedPropertyValueStrategy<T> implements PropertyValueStrategy<T> {

  @Override
  public void write(T value, DataOutputView outputView) throws IOException {
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

  /**
   * Reads data of variable size from a data input view. The size of the data is determined by the
   * type byte and {@see org.gradoop.common.model.impl.properties.PropertyValue#FLAG_LARGE}.
   *
   * @param inputView Data input view to read from
   * @param typeByte Byte indicating the type of the serialized data
   * @return The serialized data in the data input view
   * @throws IOException when reading a byte goes wrong
   */
  byte[] readVariableSizedData(DataInputView inputView, byte typeByte) throws IOException {
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

    return rawBytes;
  }

  /**
   * Creates an instance of {@link DataInputViewStreamWrapper} from a byte array.
   *
   * @param bytes input byte array
   * @return A DataInputViewStreamWrapper
   */
  DataInputViewStreamWrapper createInputView(byte[] bytes) {
    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
    DataInputStream inputStream = new DataInputStream(byteStream);
    return new DataInputViewStreamWrapper(inputStream);
  }
}
