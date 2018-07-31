/*
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
package org.gradoop.storage.impl.hbase.iterator;

import org.apache.hadoop.io.Writable;
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
   * @throws IOException if writing to output fails
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    value.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    value.read(dataInput);
  }
}
