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
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;
import java.io.IOException;

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type
 * {@code Double}.
 */
public class DoubleStrategy implements PropertyValueStrategy<Double> {

  @Override
<<<<<<< HEAD
  public boolean write(Double value, DataOutputView outputView) throws IOException {
    outputView.write(getRawBytes(value));
    return true;
=======
  public void write(Double value, DataOutputView outputView) throws IOException {
    outputView.write(getRawBytes(value));
>>>>>>> c737b343803e03061d2f3bfc6894d5e91c2d7b51
  }

  @Override
  public Double read(DataInputView inputView, byte typeByte) throws IOException {
    int length = Bytes.SIZEOF_DOUBLE;
    byte[] rawBytes = new byte[length];
<<<<<<< HEAD
    for (int i  = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }
=======

    for (int i  = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }

>>>>>>> c737b343803e03061d2f3bfc6894d5e91c2d7b51
    return Bytes.toDouble(rawBytes);
  }

  @Override
  public int compare(Double value, Object other) {
    if (other instanceof Number) {
      Number num = (Number) other;
      return PropertyValueStrategyUtils.compareNumerical(value, num);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof Double;
  }

  @Override
  public Class<Double> getType() {
    return Double.class;
  }

  @Override
  public Double get(byte[] bytes) {
    return Bytes.toDouble(bytes, PropertyValue.OFFSET);
  }

  @Override
  public Byte getRawType() {
    return PropertyValue.TYPE_DOUBLE;
  }

  @Override
  public byte[] getRawBytes(Double value) {
    byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_DOUBLE];
    rawBytes[0] = getRawType();
    Bytes.putDouble(rawBytes, PropertyValue.OFFSET, value);
    return rawBytes;
  }
}