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
package org.gradoop.common.model.api.strategies;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Interface defining the methods necessary to handle the (de-) serialization of a
 * {@code PropertyValue}'s value.
 *
 * @param <T> Generic
 */
public interface PropertyValueStrategy<T> {

  /**
   * Writes the given value to the provided {@code DataOutputView}.
   *
   * @param value      to be written to the {@code DataOutputView}.
   * @param outputView that the value is written to.
   * @throws IOException if write process fails.
   */
  void write(T value, DataOutputView outputView) throws IOException;

  /**
   * Reads raw bytes from the given {@code DataInputView} and deserializes the contained object.
   *
   * @param inputView containing serialized object.
   * @param typeByte  byte needed to indicate whether serialized object has a variable length.
   * @return deserialized object.
   * @throws IOException of deserialization fails.
   */
  T read(DataInputView inputView, byte typeByte) throws IOException;

  /**
   * Compares two objects.
   *
   * @param value first object.
   * @param other second object.
   * @return a negative integer, zero, or a positive integer as first object is less than, equal to,
   * or greater than the second object.
   */
  int compare(T value, Object other);

  /**
   * Checks if given object is an instance of the data type the specific strategy handles.
   *
   * @param value to be checked.
   * @return true if {@code value} is an instance of the data type this strategy handles.
   * False otherwise.
   */
  boolean is(Object value);

  /**
   * Gets the class of the data type the specific strategy handles.
   *
   * @return some class <T>.
   */
  Class<T> getType();

  /**
   * Deserializes an object from the provided byte array.
   *
   * @param bytes representing a serialized object.
   * @return object which is an instance of the class <T>
   */
  T get(byte[] bytes);

  /**
   * Gets a byte which represents the data type the specific strategy handles.
   *
   * @return a byte.
   */
  Byte getRawType();

  /**
   * Serializes the given object.
   *
   * @param value the object to be serialized.
   * @return byte array representation of the provided object.
   */
  byte[] getRawBytes(T value);
}