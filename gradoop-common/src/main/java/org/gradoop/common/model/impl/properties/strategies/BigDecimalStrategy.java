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
import java.math.BigDecimal;

/**
 * Strategy class for handling {@link PropertyValue} operations with a value of the type
 * {@link BigDecimal}.
 */
public class BigDecimalStrategy extends AbstractVariableSizedPropertyValueStrategy<BigDecimal> {

  @Override
  public BigDecimal read(DataInputView inputView, byte typeByte) throws IOException {
    byte[] rawBytes = readVariableSizedData(inputView, typeByte);
    return Bytes.toBigDecimal(rawBytes);
  }

  @Override
  public int compare(BigDecimal value, Object other) {
    if (other instanceof Number) {
      Number num = (Number) other;
      return PropertyValueStrategyUtils.compareNumerical(value, num);
    }
    throw new IllegalArgumentException(String.format(
      "Incompatible types: %s, %s", value.getClass(), other.getClass()));
  }

  @Override
  public boolean is(Object value) {
    return value instanceof BigDecimal;
  }

  @Override
  public Class<BigDecimal> getType() {
    return BigDecimal.class;
  }

  @Override
  public BigDecimal get(byte[] bytes) {
    return Bytes.toBigDecimal(bytes, PropertyValue.OFFSET, bytes.length - PropertyValue.OFFSET);
  }

  @Override
  public byte getRawType() {
    return Type.BIG_DECIMAL.getTypeByte();
  }

  @Override
  public byte[] getRawBytes(BigDecimal value) {
    byte[] valueBytes = Bytes.toBytes(value);
    byte[] rawBytes = new byte[PropertyValue.OFFSET + valueBytes.length];
    rawBytes[0] = getRawType();
    Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
    return rawBytes;
  }
}
