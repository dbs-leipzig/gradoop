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
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.Type;

import java.io.IOException;

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type
 * {@code Integer}.
 */
public class IntegerStrategy extends AbstractFixSizedPropertyValueStrategy<Integer> {

  @Override
  public Integer read(DataInputView inputView, byte typeByte) throws IOException {
    int length = Bytes.SIZEOF_INT;
    byte[] rawBytes = new byte[length];

    for (int i  = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }

    return Bytes.toInt(rawBytes);
  }

  @Override
  public int compare(Integer value, Object other) {
    return PropertyValueStrategyUtils.compareNumerical(value, other);
  }

  @Override
  public boolean is(Object value) {
    return value instanceof Integer;
  }

  @Override
  public Class<Integer> getType() {
    return Integer.class;
  }

  @Override
  public Integer get(byte[] bytes) {
    return Bytes.toInt(bytes, PropertyValue.OFFSET);
  }

  @Override
  public byte getRawType() {
    return Type.INTEGER.asByte();
  }

  @Override
  public byte[] getRawBytes(Integer value) {
    byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_INT];
    rawBytes[0] = getRawType();
    Bytes.putInt(rawBytes, PropertyValue.OFFSET, value);
    return rawBytes;
  }
}
