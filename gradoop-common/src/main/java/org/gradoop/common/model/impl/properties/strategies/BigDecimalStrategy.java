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
import java.math.BigDecimal;
import java.util.Arrays;

/**
 * Strategy class for handling {@code PropertyValue} operations with a value of the type
 * {@code BigDecimal}.
 */
public class BigDecimalStrategy implements PropertyValueStrategy<BigDecimal> {

  @Override
  public void write(BigDecimal value, DataOutputView outputView) throws IOException {
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

  @Override
  public BigDecimal read(DataInputView inputView, byte typeByte) throws IOException {
    int length;
    // read length
    if ((typeByte & PropertyValue.FLAG_LARGE) == PropertyValue.FLAG_LARGE) {
      length = inputView.readInt();
    } else {
      length = inputView.readShort();
    }
    byte[] rawBytes = new byte[length];
    for (int i = 0; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }
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

    BigDecimal decimal;
    byte type = bytes[0];
    byte[] valueBytes = Arrays.copyOfRange(bytes, PropertyValue.OFFSET, bytes.length);

    switch (type) {
    case PropertyValue.TYPE_BIG_DECIMAL:
      decimal = Bytes.toBigDecimal(valueBytes);
      break;
    case PropertyValue.TYPE_FLOAT:
      decimal = BigDecimal.valueOf(Bytes.toFloat(valueBytes));
      break;
    case PropertyValue.TYPE_DOUBLE:
      decimal = BigDecimal.valueOf(Bytes.toDouble(valueBytes));
      break;
    case PropertyValue.TYPE_SHORT:
      decimal = BigDecimal.valueOf(Bytes.toShort(valueBytes));
      break;
    case PropertyValue.TYPE_INTEGER:
      decimal = BigDecimal.valueOf(Bytes.toInt(valueBytes));
      break;
    case PropertyValue.TYPE_LONG:
      decimal = BigDecimal.valueOf(Bytes.toLong(valueBytes));
      break;
    case PropertyValue.TYPE_STRING:
      decimal = new BigDecimal(Bytes.toString(valueBytes));
      break;
    default:
      throw new ClassCastException(
      "Cannot covert " + this.getType().getSimpleName() +
      " to " + BigDecimal.class.getSimpleName());
    }
    return decimal;
  }

  @Override
  public Byte getRawType() {
    return PropertyValue.TYPE_BIG_DECIMAL;
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
